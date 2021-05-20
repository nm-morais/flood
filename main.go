package main

import (
	"flag"
	"flood/application"
	"flood/flood"
	"flood/plumtree"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	cyclonTMan "github.com/nm-morais/Cyclon_T-Man/protocol"
	cyclon "github.com/nm-morais/cyclon/protocol"
	demmon "github.com/nm-morais/demmon/core/membership/protocol"
	babel "github.com/nm-morais/go-babel/pkg"
	"github.com/nm-morais/go-babel/pkg/peer"
	"github.com/nm-morais/go-babel/pkg/protocol"
	hpv "github.com/nm-morais/hyparview/protocol"
	x_bot "github.com/nm-morais/x-bot/protocol"
	"github.com/ungerik/go-dry"
	"gopkg.in/yaml.v2"
)

var (
	membershipProtocol *string
	bootstraps         *string
	listenIP           *string
	logFolder          *string
	confFile           *string
	broadcastProtocol  *string
	bwScore            *int64
	useBW              *bool
	nrMessagesPerNode  *int
	payloadSize        *int
	scenarioDuration   *time.Duration
)

const (
	defaultPort          = 1200
	defaultAnalyticsPort = 1300

	HyparviewName  = "hyparview"
	cyclonTmanName = "Cyclon_T-Man"
	cyclonName     = "cyclon"
	xBotName       = "x-bot"
	demmonName     = "demmon"

	floodName    = "flood"
	plumTreeName = "plumTree"
)

func main() {

	membershipProtocol = flag.String("membership", "", "choose membership protocol (hyparview, Cyclon_T-Man, x-bot, demmon)")
	broadcastProtocol = flag.String("broadcast", "", "the broadcast algorithm")
	bootstraps = flag.String("bootstraps", "", "choose custom bootstrap nodes (space-separated ip:port list)")
	listenIP = flag.String("listenIP", "", "choose custom ip to listen to")
	logFolder = flag.String("logfolder", "/tmp/logs/", "folder to save logs to")
	confFile = flag.String("conf", "", "the path to the config file")
	bwScore = flag.Int64("bandwidth", 0, "bandwidth score")
	useBW = flag.Bool("useBW", false, "if bandwidths are used")
	nrMessagesPerNode = flag.Int("msgsPerNode", 30, "Number of messages each node sends")
	scenarioDuration = flag.Duration("scenarioDuration", 1*time.Minute, "The time to send the configured messages")
	payloadSize = flag.Int("payloadSize", 10_000, "The number of bytes of the payload")

	fmt.Println("ARGS:", os.Args)
	flag.Parse()
	selfPeer := peer.NewPeer(net.ParseIP(*listenIP), defaultPort, defaultAnalyticsPort)
	*logFolder += fmt.Sprintf("%s:%d/", selfPeer.IP(), selfPeer.ProtosPort())
	protoManagerConf := babel.Config{
		Silent:           false,
		LogFolder:        *logFolder,
		HandshakeTimeout: 8 * time.Second,
		SmConf: babel.StreamManagerConf{
			BatchMaxSizeBytes: 65_534,
			BatchTimeout:      300 * time.Millisecond,
			DialTimeout:       3 * time.Second,
			WriteTimeout:      3 * time.Second,
		},
		Peer:     selfPeer,
		PoolSize: 0,
	}
	nwConf := &babel.NodeWatcherConf{
		PrintLatencyToInterval:    10 * time.Second,
		EvalConditionTickDuration: 1500 * time.Millisecond,
		MaxRedials:                2,
		TcpTestTimeout:            10 * time.Second,
		UdpTestTimeout:            10 * time.Second,
		NrTestMessagesToSend:      2,
		NrMessagesWithoutWait:     3,
		NrTestMessagesToReceive:   2,
		HbTickDuration:            1000 * time.Millisecond,
		MinSamplesLatencyEstimate: 3,
		OldLatencyWeight:          0.9,
		NewLatencyWeight:          0.1,
		PhiThreshold:              8.0,
		WindowSize:                20,
		MinStdDeviation:           500 * time.Millisecond,
		AcceptableHbPause:         1500 * time.Millisecond,
		FirstHeartbeatEstimate:    1500 * time.Millisecond,

		AdvertiseListenAddr: selfPeer.ToTCPAddr().IP,
		ListenAddr:          selfPeer.ToTCPAddr().IP,
		ListenPort:          int(selfPeer.AnalyticsPort()),
	}

	p := babel.NewProtoManager(protoManagerConf)
	bootstrapsParsed := ParseBootstrapArg(bootstraps)
	isLandmark := false
	switch *membershipProtocol {

	case xBotName:
		fmt.Println("Protocol being used is: Hyparview and X-Bot")
		conf := &x_bot.XBotConfig{}
		readConfFile(*confFile, conf)
		conf.SelfPeer = struct {
			AnalyticsPort int    `yaml:"analyticsPort"`
			Port          int    `yaml:"port"`
			Host          string `yaml:"host"`
		}{
			AnalyticsPort: int(selfPeer.AnalyticsPort()),
			Port:          int(selfPeer.ProtosPort()),
			Host:          selfPeer.IP().String(),
		}
		conf.BootstrapPeers = bootstrapsParsed
		nw := babel.NewNodeWatcher(
			*nwConf,
			p,
		)
		p.RegisterNodeWatcher(nw)
		p.RegisterProtocol(x_bot.NewXBotProtocol(p, nw, conf))
	case cyclonTmanName:
		fmt.Println("Protocol being used is: Cyclon and T-Man")
		conf := &cyclonTMan.CyclonTManConfig{}
		readConfFile(*confFile, conf)
		conf.SelfPeer = struct {
			AnalyticsPort int    `yaml:"analyticsPort"`
			Port          int    `yaml:"port"`
			Host          string `yaml:"host"`
		}{
			AnalyticsPort: int(selfPeer.AnalyticsPort()),
			Port:          int(selfPeer.ProtosPort()),
			Host:          selfPeer.IP().String(),
		}
		conf.BootstrapPeers = bootstrapsParsed
		nw := babel.NewNodeWatcher(
			*nwConf,
			p,
		)
		p.RegisterNodeWatcher(nw)
		p.RegisterProtocol(cyclonTMan.NewCyclonTManProtocol(p, nw, conf))
	case cyclonName:
		fmt.Println("Protocol being used is: Cyclon")
		conf := &cyclon.CyclonConfig{}
		readConfFile(*confFile, conf)
		conf.SelfPeer = struct {
			AnalyticsPort int    `yaml:"analyticsPort"`
			Port          int    `yaml:"port"`
			Host          string `yaml:"host"`
		}{
			AnalyticsPort: int(selfPeer.AnalyticsPort()),
			Port:          int(selfPeer.ProtosPort()),
			Host:          selfPeer.IP().String(),
		}
		conf.BootstrapPeers = bootstrapsParsed
		p.RegisterProtocol(cyclon.NewCyclonProtocol(p, conf))
	case HyparviewName:
		fmt.Println("Protocol being used is: Hyparview")
		conf := &hpv.HyparviewConfig{}
		readConfFile(*confFile, conf)
		conf.SelfPeer = struct {
			AnalyticsPort int    `yaml:"analyticsPort"`
			Port          int    `yaml:"port"`
			Host          string `yaml:"host"`
		}{
			AnalyticsPort: int(selfPeer.AnalyticsPort()),
			Port:          int(selfPeer.ProtosPort()),
			Host:          selfPeer.IP().String(),
		}
		conf.BootstrapPeers = bootstrapsParsed
		p.RegisterProtocol(hpv.NewHyparviewProtocol(p, conf))
	case demmonName:
		fmt.Println("Protocol being used is: Demmon")

		landmarks := make([]*demmon.PeerWithIDChain, 0, len(bootstrapsParsed))
		nw := babel.NewNodeWatcher(
			*nwConf,
			p,
		)
		for i, b := range bootstrapsParsed {
			landmark := demmon.NewPeerWithIDChain(
				demmon.PeerIDChain{demmon.PeerID{uint8(i)}},
				peer.NewPeer(net.ParseIP(b.Host), uint16(b.Port), uint16(b.AnalyticsPort)),
				0,
				0,
				make(demmon.Coordinates, len(bootstrapsParsed)),
				0,
				0,
			)
			landmarks = append(landmarks, landmark)
			if peer.PeersEqual(landmark, selfPeer) {
				isLandmark = true
			}
		}
		conf := &demmon.DemmonTreeConfig{
			MaxDiffForBWScore:                        5000,
			BandwidthScore:                           int(*bwScore),
			UseBwScore:                               *useBW,
			Landmarks:                                landmarks,
			LandmarkRedialTimer:                      5 * time.Second,
			JoinMessageTimeout:                       10 * time.Second,
			MaxRetriesJoinMsg:                        3,
			ParentRefreshTickDuration:                5 * time.Second,
			ChildrenRefreshTickDuration:              5 * time.Second,
			RejoinTimerDuration:                      10 * time.Second,
			NrPeersToBecomeChildrenPerParentInAbsorb: 3,
			NrPeersToBecomeParentInAbsorb:            2,
			PhiLevelForNodeDown:                      8,
			MaxPeersInEView:                          20,
			EmitWalkTimeout:                          8 * time.Second,
			NrHopsToIgnoreWalk:                       2,
			RandomWalkTTL:                            6,
			BiasedWalkTTL:                            5,
			NrPeersInWalkMessage:                     15,
			NrPeersToMergeInWalkSample:               5,
			NrPeersToMeasureBiased:                   2,
			NrPeersToMeasureRandom:                   1,
			MeasureNewPeersRefreshTickDuration:       7 * time.Second,
			MaxMeasuredPeers:                         15,
			MinLatencyImprovementToImprovePosition:   50 * time.Millisecond,
			CheckChildenSizeTimerDuration:            7 * time.Second,
			EmitWalkProbability:                      0.33,
			BiasedWalkProbability:                    0.2,
			AttemptImprovePositionProbability:        0.5,
			MinGrpSize:                               2,
			MaxGrpSize:                               9,
			UnderpopulatedGroupTimerDuration:         12 * time.Second,
		}

		p.RegisterProtocol(demmon.New(conf, p, nw))
	default:
		panic("unrecognized protocol")
	}

	p.RegisterListenAddr(selfPeer.ToTCPAddr())
	p.RegisterListenAddr(selfPeer.ToUDPAddr())

	if *membershipProtocol == demmonName {
		if *broadcastProtocol != floodName {
			panic("Unrecognized broadcast protocol for demmon")
		}
	}

	var floodProto protocol.Protocol
	if *broadcastProtocol == floodName {
		floodProto = flood.NewFloodProtocol(p, *membershipProtocol == cyclonTmanName || *membershipProtocol == cyclonName, *membershipProtocol == demmonName, isLandmark)
	} else if *broadcastProtocol == plumTreeName {
		floodProto = plumtree.NewPlumTreeProtocol(p, *membershipProtocol == cyclonTmanName || *membershipProtocol == cyclonName)
	} else {
		panic("Unrecognized broadcast protocol")
	}
	isPlumtreeRoot := bootstrapsParsed[0].Host == selfPeer.IP().String()

	p.RegisterProtocol(floodProto)
	p.RegisterProtocol(application.NewApplicationProtocol(p, isPlumtreeRoot, floodProto.ID(), *logFolder, "bcastStats.csv", *nrMessagesPerNode, *payloadSize, *scenarioDuration))
	p.StartSync()
}

func readConfFile(path string, conf interface{}) interface{} {
	configFileName := path
	envVars := dry.EnvironMap()
	customConfig, ok := envVars["config"]
	if ok {
		configFileName = customConfig
	}
	f, err := os.Open(configFileName)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(conf)
	if err != nil {
		panic(err)
	}
	return conf
}

func ParseBootstrapArg(arg *string) []struct {
	Port          int    `yaml:"port"`
	Host          string `yaml:"host"`
	AnalyticsPort int    `yaml:"analyticsPort"`
} {
	bootstrapPeers := []struct {
		Port          int    `yaml:"port"`
		Host          string `yaml:"host"`
		AnalyticsPort int    `yaml:"analyticsPort"`
	}{}
	if arg != nil && *arg != "" {
		fmt.Println("Setting custom bootstrap nodes")
		for _, ipPortStr := range strings.Split(*arg, " ") {
			split := strings.Split(ipPortStr, ":")
			ip := split[0]

			// assume all peers are running in same port if port is not specified
			var portInt int = defaultPort
			var analyticsPortInt int = defaultAnalyticsPort

			if len(split) > 1 {
				portIntAux, err := strconv.ParseInt(split[1], 10, 32)
				if err != nil {
					panic(err)
				}
				portInt = int(portIntAux)
			}

			if len(split) > 2 {
				portIntAux, err := strconv.ParseInt(split[2], 10, 32)
				if err != nil {
					panic(err)
				}
				analyticsPortInt = int(portIntAux)
			}

			bootstrapPeers = append(bootstrapPeers, struct {
				Port          int    `yaml:"port"`
				Host          string `yaml:"host"`
				AnalyticsPort int    `yaml:"analyticsPort"`
			}{
				Host:          ip,
				Port:          portInt,
				AnalyticsPort: analyticsPortInt,
			})
		}
	}
	return bootstrapPeers
}
