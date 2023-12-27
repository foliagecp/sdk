

package cache

type Task interface {
	Process(s *Store) error
}

type SetTask struct {
	keyPath  []string
	value    []byte
	revision uint64
}

func newSetTask(key string, value []byte, rev uint64) *SetTask {
	return &SetTask{
		keyPath:  storeValuePathFromStr(key),
		value:    value,
		revision: rev,
	}
}

func (t *SetTask) Process(s *Store) error {
	if err := s.root.MkvalueAll(t.keyPath).Update(t.value, t.revision); err != nil {
		return err
	}

	return nil
}

type DeleteTask struct {
	keyPath []string
}

func newDeleteTask(key string) *DeleteTask {
	return &DeleteTask{
		keyPath: storeValuePathFromStr(key),
	}
}

func (t *DeleteTask) Process(s *Store) error {
	value, err := s.root.SearchChild(t.keyPath)
	if err != nil {
		return err
	}

	value.destroy()

	return nil
}
