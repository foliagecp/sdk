# CMDB Topology

```mermaid
graph TD
    %% Navigation root
    NAV[NAV<br/>navigation]
    
    %% Datacenter Level
    DC1[DC-East<br/>datacenter1]
    DC2[DC-West<br/>datacenter2]
    
    %% Rack Level
    R1[Rack-A01<br/>rack1]
    R2[Rack-A02<br/>rack2]
    R3[Rack-B01<br/>rack3]
    R4[Rack-B02<br/>rack4]
    
    %% Servers
    S1[web-01<br/>server1]
    S2[web-02<br/>server2]
    S3[db-01<br/>server3]
    S4[db-02<br/>server4]
    S5[app-01<br/>server5]
    S6[app-02<br/>server6]
    S7[mon-01<br/>server7]
    S8[backup-01<br/>server8]
    
    %% Storage
    ST1[SAN-Primary<br/>storage1]
    ST2[SAN-Secondary<br/>storage2]
    ST3[NAS-Backup<br/>storage3]
    ST4[Local-Cache<br/>storage4]
    
    %% Network
    N1[Core-Switch-01<br/>network1]
    N2[TOR-Switch-A01<br/>network2]
    N3[TOR-Switch-A02<br/>network3]
    N4[TOR-Switch-B01<br/>network4]
    N5[TOR-Switch-B02<br/>network5]
    
    %% Sensors
    SE1[temp-sensor<br/>sensor1]
    SE2[humidity-sensor<br/>sensor2]
    SE3[power-sensor-A01<br/>sensor3]
    SE4[power-sensor-A02<br/>sensor4]
    SE5[vibration-sensor<br/>sensor5]
    
    %% NAV connections to top level - need to add this to code
    NAV ---|to_datacenter| DC1
    NAV ---|to_datacenter| DC2
    
    %% Physical containment (contains/physical)
    DC1 ---|to_rack<br/>contains<br/>physical| R1
    DC1 ---|to_rack<br/>contains<br/>physical| R2
    DC2 ---|to_rack<br/>contains<br/>physical| R3
    DC2 ---|to_rack<br/>contains<br/>physical| R4
    
    %% Rack hosting (hosts/physical)
    R1 ---|to_server<br/>hosts<br/>physical| S1
    R1 ---|to_server<br/>hosts<br/>physical| S2
    R1 ---|to_storage<br/>hosts<br/>physical| ST1
    R1 ---|to_network<br/>hosts<br/>physical| N2
    
    R2 ---|to_server<br/>hosts<br/>physical| S3
    R2 ---|to_server<br/>hosts<br/>physical| S4
    R2 ---|to_storage<br/>hosts<br/>physical| ST2
    R2 ---|to_network<br/>hosts<br/>physical| N3
    
    R3 ---|to_server<br/>hosts<br/>physical| S5
    R3 ---|to_server<br/>hosts<br/>physical| S6
    R3 ---|to_storage<br/>hosts<br/>physical| ST4
    R3 ---|to_network<br/>hosts<br/>physical| N4
    
    R4 ---|to_server<br/>hosts<br/>physical| S7
    R4 ---|to_server<br/>hosts<br/>physical| S8
    R4 ---|to_storage<br/>hosts<br/>physical| ST3
    R4 ---|to_network<br/>hosts<br/>physical| N5
    
    %% Server to Storage connections (connects_to/logical)
    S1 ---|to_storage<br/>connects_to<br/>logical| ST1
    S2 ---|to_storage<br/>connects_to<br/>logical| ST1
    S3 ---|to_storage<br/>connects_to<br/>logical| ST2
    S4 ---|to_storage<br/>connects_to<br/>logical| ST2
    S5 ---|to_storage<br/>connects_to<br/>logical| ST4
    S6 ---|to_storage<br/>connects_to<br/>logical| ST4
    
    %% Server to Network connections (connects_to/logical)
    S1 ---|to_network<br/>connects_to<br/>logical| N2
    S2 ---|to_network<br/>connects_to<br/>logical| N2
    S3 ---|to_network<br/>connects_to<br/>logical| N3
    S4 ---|to_network<br/>connects_to<br/>logical| N3
    
    %% Network uplinks (uplinks_to/logical)
    N2 ---|to_network<br/>uplinks_to<br/>logical| N1
    N3 ---|to_network<br/>uplinks_to<br/>logical| N1
    N4 ---|to_network<br/>uplinks_to<br/>logical| N1
    N5 ---|to_network<br/>uplinks_to<br/>logical| N1
    
    %% Database to Storage (stores_on/persistence) - removed this wrong section
    
    %% Monitoring (monitors/monitoring)
    SE1 ---|to_datacenter<br/>monitors<br/>monitoring| DC1
    SE2 ---|to_datacenter<br/>monitors<br/>monitoring| DC1
    SE3 ---|to_rack<br/>monitors<br/>monitoring| R1
    SE4 ---|to_rack<br/>monitors<br/>monitoring| R2
    SE5 ---|to_datacenter<br/>monitors<br/>monitoring| DC1
    
    %% Styling with better readability
    classDef nav fill:#f8fafc,stroke:#475569,stroke-width:3px,color:#1e293b
    classDef datacenter fill:#e8f4f8,stroke:#2563eb,stroke-width:2px,color:#1e40af
    classDef rack fill:#f0f9ff,stroke:#0ea5e9,stroke-width:2px,color:#0c4a6e
    classDef server fill:#ecfdf5,stroke:#10b981,stroke-width:2px,color:#064e3b
    classDef storage fill:#fef3c7,stroke:#f59e0b,stroke-width:2px,color:#92400e
    classDef network fill:#f3e8ff,stroke:#8b5cf6,stroke-width:2px,color:#581c87
    classDef sensor fill:#fef2f2,stroke:#ef4444,stroke-width:2px,color:#991b1b
    
    class NAV nav
    class DC1,DC2 datacenter
    class R1,R2,R3,R4 rack
    class S1,S2,S3,S4,S5,S6,S7,S8 server
    class ST1,ST2,ST3,ST4 storage
    class N1,N2,N3,N4,N5 network
    class SE1,SE2,SE3,SE4,SE5 sensor