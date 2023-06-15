// Go port of Coda Hale's Metrics library
//
// <https://github.com/rcrowley/go-metrics>
//
// Coda Hale's original work: <https://github.com/codahale/metrics>
package metrics

import (
	"os"
	"runtime/metrics"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/log"
)

// Enabled is checked by the constructor functions for all of the
// standard metrics. If it is true, the metric returned is a stub.
//
// This global kill-switch helps quantify the observer effect and makes
// for less cluttered pprof profiles.
var Enabled = false

// EnabledExpensive is a soft-flag meant for external packages to check if costly
// metrics gathering is allowed or not. The goal is to separate standard metrics
// for health monitoring and debug metrics that might impact runtime performance.
var EnabledExpensive = false

// enablerFlags is the CLI flag names to use to enable metrics collections.
var enablerFlags = []string{"metrics"}

// expensiveEnablerFlags is the CLI flag names to use to enable metrics collections.
var expensiveEnablerFlags = []string{"metrics.expensive"}

// Init enables or disables the metrics system. Since we need this to run before
// any other code gets to create meters and timers, we'll actually do an ugly hack
// and peek into the command line args for the metrics flag.
func init() {
	for _, arg := range os.Args {
		flag := strings.TrimLeft(arg, "-")

		for _, enabler := range enablerFlags {
			if !Enabled && flag == enabler {
				log.Info("Enabling metrics collection")
				Enabled = true
			}
		}
		for _, enabler := range expensiveEnablerFlags {
			if !EnabledExpensive && flag == enabler {
				log.Info("Enabling expensive metrics collection")
				EnabledExpensive = true
			}
		}
	}
}

var threadCreateProfile = pprof.Lookup("threadcreate")

type runtimeStats struct {
	CPUTotal uint64
	CPUUser  uint64
	CPUIdle  uint64
	CPUScan  uint64
	CPUGC    uint64

	GCCycle       uint64
	GCPauses      *metrics.Float64Histogram
	GCAllocBytes  uint64
	GCFreedBytes  uint64
	GCTargetBytes uint64

	MemTotal     uint64
	HeapObjects  uint64
	HeapFree     uint64
	HeapReleased uint64
	HeapUnused   uint64

	Goroutines   uint64
	SchedLatency *metrics.Float64Histogram
}

var runtimeSamples = []metrics.Sample{
	{Name: "/cpu/classes/total:cpu-seconds"},
	{Name: "/cpu/classes/user:cpu-seconds"},
	{Name: "/cpu/classes/idle:cpu-seconds"},

	// Estimated total CPU time spent performing tasks that return
	// unused memory to the underlying platform.
	{Name: "/cpu/classes/scavenge/total:cpu-seconds"},
	{Name: "/cpu/classes/gc/total:cpu-seconds"},

	{Name: "/gc/cycles/total:gc-cycles"},
	{Name: "/gc/pauses:seconds"}, // histogram
	{Name: "/gc/heap/allocs:bytes"},
	{Name: "/gc/heap/frees:bytes"},
	{Name: "/gc/heap/goal:bytes"},

	{Name: "/memory/classes/total:bytes"},
	{Name: "/memory/classes/heap/objects:bytes"},
	{Name: "/memory/classes/heap/free:bytes"},
	{Name: "/memory/classes/heap/released:bytes"},
	{Name: "/memory/classes/heap/unused:bytes"},
	{Name: "/sched/goroutines:goroutines"},
	{Name: "/sched/latencies:seconds"}, // histogram
}

func readRuntimeStats(v *runtimeStats) {
	metrics.Read(runtimeSamples)
	for _, s := range runtimeSamples {
		// Skip invalid/unknown metrics. This is needed because some metrics
		// are unavailable in older Go versions, and attempting to read a 'bad'
		// metric panics.
		if s.Value.Kind() == metrics.KindBad {
			continue
		}

		switch s.Name {
		case "/cpu/classes/total:cpu-seconds":
			log.Info("Metrics type", "item", s.Name, "type", s.Value.Kind())
			//v.CPUTotal = s.Value.Uint64()
		case "/cpu/classes/user:cpu-seconds":
			log.Info("Metrics type", "item", s.Name, "type", s.Value.Kind())
			//v.CPUUser = s.Value.Uint64()
		case "/cpu/classes/idle:cpu-seconds":
			log.Info("Metrics type", "item", s.Name, "type", s.Value.Kind())
			//v.CPUIdle = s.Value.Uint64()
		case "/cpu/classes/scavenge/total:cpu-seconds":
			log.Info("Metrics type", "item", s.Name, "type", s.Value.Kind())
			//v.CPUScan = s.Value.Uint64()
		case "/cpu/classes/gc/total:cpu-seconds":
			log.Info("Metrics type", "item", s.Name, "type", s.Value.Kind())
			//v.CPUGC = s.Value.Uint64()

		case "/gc/cycles/total:gc-cycles":
			//v.GCCycle = s.Value.Uint64()
			log.Info("Metrics type", "item", s.Name, "type", s.Value.Kind())
		case "/gc/pauses:seconds":
			v.GCPauses = s.Value.Float64Histogram()
		case "/gc/heap/allocs:bytes":
			v.GCAllocBytes = s.Value.Uint64()
		case "/gc/heap/frees:bytes":
			v.GCFreedBytes = s.Value.Uint64()
		case "/gc/heap/goal:bytes":
			v.GCTargetBytes = s.Value.Uint64()
		case "/memory/classes/total:bytes":
			v.MemTotal = s.Value.Uint64()
		case "/memory/classes/heap/objects:bytes":
			v.HeapObjects = s.Value.Uint64()
		case "/memory/classes/heap/free:bytes":
			v.HeapFree = s.Value.Uint64()
		case "/memory/classes/heap/released:bytes":
			v.HeapReleased = s.Value.Uint64()
		case "/memory/classes/heap/unused:bytes":
			v.HeapUnused = s.Value.Uint64()
		case "/sched/goroutines:goroutines":
			v.Goroutines = s.Value.Uint64()
		case "/sched/latencies:seconds":
			v.SchedLatency = s.Value.Float64Histogram()
		}
	}
}

// CollectProcessMetrics periodically collects various metrics about the running process.
func CollectProcessMetrics(refresh time.Duration) {
	// Short circuit if the metrics system is disabled
	if !Enabled {
		return
	}

	// Create the various data collectors
	var (
		cpustats  = make([]CPUStats, 2)
		diskstats = make([]DiskStats, 2)
		rstats    = make([]runtimeStats, 2)
	)

	// This scale factor is used for the runtime's time metrics. It's useful to convert to
	// ns here because the runtime gives times in float seconds, but runtimeHistogram can
	// only provide integers for the minimum and maximum values.
	const secondsToNs = float64(time.Second)

	// Define the various metrics to collect
	var (
		cpuSysLoad            = GetOrRegisterGauge("system/cpu/sysload", DefaultRegistry)
		cpuSysWait            = GetOrRegisterGauge("system/cpu/syswait", DefaultRegistry)
		cpuProcLoad           = GetOrRegisterGauge("system/cpu/procload", DefaultRegistry)
		cpuSysLoadTotal       = GetOrRegisterCounterFloat64("system/cpu/sysload/total", DefaultRegistry)
		cpuSysWaitTotal       = GetOrRegisterCounterFloat64("system/cpu/syswait/total", DefaultRegistry)
		cpuProcLoadTotal      = GetOrRegisterCounterFloat64("system/cpu/procload/total", DefaultRegistry)
		cpuTotal              = GetOrRegisterGauge("system/cpu/total", DefaultRegistry)
		cpuUser               = GetOrRegisterGauge("system/cpu/user", DefaultRegistry)
		cpuIdle               = GetOrRegisterGauge("system/cpu/idle", DefaultRegistry)
		cpuGC                 = GetOrRegisterGauge("system/cpu/gc/time", DefaultRegistry)
		cpuScan               = GetOrRegisterGauge("system/cpu/scan", DefaultRegistry)
		gcCycles              = GetOrRegisterMeter("system/cpu/gc/cycle", DefaultRegistry)
		gcTarget              = GetOrRegisterGauge("system/cpu/gc/target", DefaultRegistry)
		cpuThreads            = GetOrRegisterGauge("system/cpu/threads", DefaultRegistry)
		cpuGoroutines         = GetOrRegisterGauge("system/cpu/goroutines", DefaultRegistry)
		cpuSchedLatency       = getOrRegisterRuntimeHistogram("system/cpu/schedlatency", secondsToNs, nil)
		memPauses             = getOrRegisterRuntimeHistogram("system/memory/pauses", secondsToNs, nil)
		memAllocs             = GetOrRegisterMeter("system/memory/allocs", DefaultRegistry)
		memFrees              = GetOrRegisterMeter("system/memory/frees", DefaultRegistry)
		memTotal              = GetOrRegisterGauge("system/memory/held", DefaultRegistry)
		heapUsed              = GetOrRegisterGauge("system/memory/used", DefaultRegistry)
		heapObjects           = GetOrRegisterGauge("system/memory/objects", DefaultRegistry)
		diskReads             = GetOrRegisterMeter("system/disk/readcount", DefaultRegistry)
		diskReadBytes         = GetOrRegisterMeter("system/disk/readdata", DefaultRegistry)
		diskReadBytesCounter  = GetOrRegisterCounter("system/disk/readbytes", DefaultRegistry)
		diskWrites            = GetOrRegisterMeter("system/disk/writecount", DefaultRegistry)
		diskWriteBytes        = GetOrRegisterMeter("system/disk/writedata", DefaultRegistry)
		diskWriteBytesCounter = GetOrRegisterCounter("system/disk/writebytes", DefaultRegistry)
	)

	var lastCollectTime time.Time

	// Iterate loading the different stats and updating the meters.
	now, prev := 0, 1
	for ; ; now, prev = prev, now {
		// Gather CPU times.
		ReadCPUStats(&cpustats[now])
		collectTime := time.Now()
		secondsSinceLastCollect := collectTime.Sub(lastCollectTime).Seconds()
		lastCollectTime = collectTime
		if secondsSinceLastCollect > 0 {
			sysLoad := cpustats[now].GlobalTime - cpustats[prev].GlobalTime
			sysWait := cpustats[now].GlobalWait - cpustats[prev].GlobalWait
			procLoad := cpustats[now].LocalTime - cpustats[prev].LocalTime
			// Convert to integer percentage.
			cpuSysLoad.Update(int64(sysLoad / secondsSinceLastCollect * 100))
			cpuSysWait.Update(int64(sysWait / secondsSinceLastCollect * 100))
			cpuProcLoad.Update(int64(procLoad / secondsSinceLastCollect * 100))
			// increment counters (ms)
			cpuSysLoadTotal.Inc(sysLoad)
			cpuSysWaitTotal.Inc(sysWait)
			cpuProcLoadTotal.Inc(procLoad)
		}

		// Threads
		cpuThreads.Update(int64(threadCreateProfile.Count()))

		// Go runtime metrics
		readRuntimeStats(&rstats[now])

		cpuTotal.Update(int64(rstats[now].CPUTotal))
		cpuIdle.Update(int64(rstats[now].CPUIdle))
		cpuUser.Update(int64(rstats[now].CPUUser))
		cpuGC.Update(int64(rstats[now].CPUGC))
		cpuScan.Update(int64(rstats[now].CPUScan))
		gcCycles.Mark(int64(rstats[now].GCCycle - rstats[prev].GCCycle))
		gcTarget.Update(int64(rstats[now].GCTargetBytes))

		cpuGoroutines.Update(int64(rstats[now].Goroutines))
		cpuSchedLatency.update(rstats[now].SchedLatency)
		memPauses.update(rstats[now].GCPauses)

		memAllocs.Mark(int64(rstats[now].GCAllocBytes - rstats[prev].GCAllocBytes))
		memFrees.Mark(int64(rstats[now].GCFreedBytes - rstats[prev].GCFreedBytes))

		memTotal.Update(int64(rstats[now].MemTotal))
		heapUsed.Update(int64(rstats[now].MemTotal - rstats[now].HeapUnused - rstats[now].HeapFree - rstats[now].HeapReleased))
		heapObjects.Update(int64(rstats[now].HeapObjects))

		// Disk
		if ReadDiskStats(&diskstats[now]) == nil {
			diskReads.Mark(diskstats[now].ReadCount - diskstats[prev].ReadCount)
			diskReadBytes.Mark(diskstats[now].ReadBytes - diskstats[prev].ReadBytes)
			diskWrites.Mark(diskstats[now].WriteCount - diskstats[prev].WriteCount)
			diskWriteBytes.Mark(diskstats[now].WriteBytes - diskstats[prev].WriteBytes)
			diskReadBytesCounter.Inc(diskstats[now].ReadBytes - diskstats[prev].ReadBytes)
			diskWriteBytesCounter.Inc(diskstats[now].WriteBytes - diskstats[prev].WriteBytes)
		}

		time.Sleep(refresh)
	}
}
