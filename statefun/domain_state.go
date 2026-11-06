package statefun

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// ErrDomainStateMismatch means persisted SDK routing belongs to a different
// domain or has incompatible addressing. Startup never repairs it implicitly.
var ErrDomainStateMismatch = errors.New("SDK domain state mismatch")

func domainMismatch(local, resource, actual, expected string) error {
	return fmt.Errorf("%w: local domain %q, %s: found %s, expected %s; restore the previous domain configuration to reuse existing data, or plan a migration; if old data is disposable, stop all writers and manually reinitialize the affected installation storage (see docs/domain_lifecycle.md); the SDK has not modified existing state", ErrDomainStateMismatch, local, resource, actual, expected)
}

// errDomainInventoryChanged identifies an uncertain snapshot, not a domain conflict.
var errDomainInventoryChanged = errors.New("domain inventory incomplete or changed while reading")

// domainInventory fails closed: the old channel-based NATS listing API hides
// errors. Count checks and bounded context prevent a partial list being used
// as evidence that conflicting state is absent. A concurrent inventory change
// is retried within the startup validation budget, never silently accepted.
func domainInventory(ctx context.Context, js nats.JetStreamContext) ([]*nats.StreamInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	before, err := js.AccountInfo(nats.Context(ctx))
	if err != nil {
		return nil, fmt.Errorf("read domain inventory: %w", err)
	}
	var list []*nats.StreamInfo
	for info := range js.Streams(nats.Context(ctx)) {
		list = append(list, info)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("list domain inventory: %w", err)
	}
	after, err := js.AccountInfo(nats.Context(ctx))
	if err != nil {
		return nil, fmt.Errorf("verify domain inventory: %w", err)
	}
	if len(list) != before.Streams || len(list) != after.Streams {
		return nil, fmt.Errorf("%w: listed %d streams, account counts %d/%d", errDomainInventoryChanged, len(list), before.Streams, after.Streams)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Config.Name < list[j].Config.Name })
	return list, nil
}

func sameSubjects(a, b []string) bool {
	a = append([]string(nil), a...)
	b = append([]string(nil), b...)
	sort.Strings(a)
	sort.Strings(b)
	return reflect.DeepEqual(a, b)
}

// ownsRouters says whether the checking runtime handles the domain routers and
// therefore creates the ingress stream with its own hub as the source prefix.
func checkDomainStream(info *nats.StreamInfo, local, hub string, ownsRouters bool) error {
	c := info.Config
	var want []string
	switch c.Name {
	case hubEventStreamName:
		want = []string{SignalPrefix + ".>"}
	case domainEgressStreamName:
		want = []string{fmt.Sprintf(DomainEgressSubjectsTmpl, local, ">")}
	case domainTraceStreamName:
		want = []string{fmt.Sprintf(domainTraceSubjectsTmpl, local)}
	case domainIngressStreamName:
		source := c.Sources
		prefix := ""
		if hub != local {
			prefix = fmt.Sprintf(streamPrefix, hub)
		}
		valid := len(source) == 1 && source[0] != nil && source[0].Name == hubEventStreamName && source[0].FilterSubject == fmt.Sprintf(FromGlobalSignalTmpl, local, ">") && len(c.Subjects) == 0 && c.Mirror == nil && c.SubjectTransform == nil && len(source[0].SubjectTransforms) == 0
		if valid {
			ext := source[0].External
			switch {
			case !ownsRouters:
				// Where hub signals come from is the router runtime's choice. A
				// runtime without routers (one that sets no hub keeps the default
				// hub) only has to find its own domain's signals routed here.
				valid = ext == nil || ext.DeliverPrefix == ""
			case prefix == "":
				valid = ext == nil
			default:
				valid = ext != nil && ext.APIPrefix == prefix && ext.DeliverPrefix == ""
			}
		}
		if !valid {
			expected := fmt.Sprintf("source=%s filter=signal.%s.> API prefix=%q", hubEventStreamName, local, prefix)
			if !ownsRouters {
				expected = fmt.Sprintf("source=%s filter=signal.%s.> (API prefix chosen by the router runtime)", hubEventStreamName, local)
			}
			return domainMismatch(local, c.Name, fmt.Sprintf("sources=%s subjects=%v mirror=%v transform=%+v", domainSourcesJSON(source), c.Subjects, c.Mirror, c.SubjectTransform), expected)
		}
		return nil
	default:
		return nil
	}
	if !sameSubjects(c.Subjects, want) || len(c.Sources) != 0 || c.Mirror != nil || c.SubjectTransform != nil {
		return domainMismatch(local, c.Name, fmt.Sprintf("subjects=%v sources=%v mirror=%v transform=%+v", c.Subjects, c.Sources, c.Mirror, c.SubjectTransform), fmt.Sprintf("subjects=%v without sources or mirror", want))
	}
	return nil
}

func checkRouterConsumers(ctx context.Context, js nats.JetStreamContext, info *nats.StreamInfo, local string) error {
	var subject string
	switch info.Config.Name {
	case domainIngressStreamName:
		subject = fmt.Sprintf(FromGlobalSignalTmpl, local, ">")
	case domainEgressStreamName:
		subject = fmt.Sprintf(DomainEgressSubjectsTmpl, local, ">")
	default:
		return nil
	}
	count := 0
	for ci := range js.ConsumersInfo(info.Config.Name, nats.Context(ctx)) {
		count++
		if strings.HasPrefix(ci.Name, info.Config.Name+"-") && strings.HasSuffix(ci.Name, "-consumer") {
			expectedName := info.Config.Name + "-" + local + "-consumer"
			if ci.Name != expectedName || ci.Config.FilterSubject != subject || len(ci.Config.FilterSubjects) != 0 || ci.Config.DeliverSubject == "" || ci.Config.DeliverGroup != expectedName+"-group" || ci.Config.AckPolicy != nats.AckExplicitPolicy {
				return domainMismatch(local, "consumer "+ci.Name, fmt.Sprintf("filter=%q filters=%v delivery=%q group=%q ack=%v", ci.Config.FilterSubject, ci.Config.FilterSubjects, ci.Config.DeliverSubject, ci.Config.DeliverGroup, ci.Config.AckPolicy), fmt.Sprintf("name=%s filter=%s", expectedName, subject))
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if count != info.State.Consumers {
		return fmt.Errorf("%w: incomplete consumer inventory for %s: listed %d, expected %d", errDomainInventoryChanged, info.Config.Name, count, info.State.Consumers)
	}
	return nil
}

func (r *Runtime) validateDomainState(ctx context.Context, cacheID string) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("validate domain state: %w", err)
		}
		err := r.validateDomainStateSnapshot(ctx, cacheID)
		if !errors.Is(err, errDomainInventoryChanged) {
			return err
		}
		// Another instance may be creating compatible resources while we
		// inspect them. Repeat the entire read-only validation; incomplete
		// snapshots never prove compatibility and real conflicts never retry.
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("validate domain state: %w; last inventory check: %w", ctx.Err(), err)
		case <-timer.C:
		}
	}
}

func (r *Runtime) validateDomainStateSnapshot(ctx context.Context, cacheID string) error {
	list, err := domainInventory(ctx, r.js)
	if err != nil {
		return err
	}
	suffix := "_" + cacheID + "_cache_bucket"
	r.ftMu.RLock()
	expected := make(map[string]string)
	for _, ft := range r.registeredFunctionTypes {
		expected[ft.getStreamName()] = ft.subject
	}
	r.ftMu.RUnlock()
	for _, info := range list {
		if err := checkDomainStream(info, r.Domain.name, r.Domain.hubDomainName, r.config.handlesDomainRouters); err != nil {
			return err
		}
		if err := checkRouterConsumers(ctx, r.js, info, r.Domain.name); err != nil {
			return err
		}
		name := info.Config.Name
		if strings.HasPrefix(name, "KV_") && !strings.HasPrefix(name, "KV_"+r.Domain.name+"_") && strings.HasSuffix(name, suffix) {
			found := strings.TrimSuffix(strings.TrimPrefix(name, "KV_"), suffix)
			if found != r.Domain.name {
				return domainMismatch(r.Domain.name, "cache "+name, fmt.Sprintf("domain=%q", found), fmt.Sprintf("domain=%q", r.Domain.name))
			}
		}
		if want, ok := expected[name]; ok && (!sameSubjects(info.Config.Subjects, []string{want}) || info.Config.Mirror != nil || len(info.Config.Sources) != 0 || info.Config.SubjectTransform != nil) {
			return domainMismatch(r.Domain.name, "function stream "+name, fmt.Sprintf("subjects=%v sources=%v mirror=%v transform=%+v", info.Config.Subjects, info.Config.Sources, info.Config.Mirror, info.Config.SubjectTransform), want)
		}
	}
	return ctx.Err()
}

func domainSourcesJSON(s []*nats.StreamSource) string { b, _ := json.Marshal(s); return string(b) }
