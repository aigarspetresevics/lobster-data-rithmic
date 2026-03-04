// NinjaTrader 8 - LOBSTER-CAP Bridge (Strategy scaffold)
// Purpose:
//   - Capture NT market events (L1 trades/quotes + L2 depth)
//   - Stream-write raw records into rotating .jsonl.gz files (incremental, NOT buffered whole-file)
//   - Keep capture durable locally for later upload to Spaces / droplet processing
//
// Notes:
//   - Attach one instance per instrument (recommended v1, per-symbol files).
//   - This is a bridge/recorder, not the analytics engine.
//   - Uses a single writer thread + queue to avoid blocking NT callbacks.
//   - Emits NT-native payloads in a LOBSTER-style typed envelope.
//
// TODOs you will likely add next:
//   - Connection status callbacks -> emit status records on disconnect/reconnect.
//   - Synthetic snapshot_ladder emission using cached depth state at rotation boundaries.
//   - Uploader sidecar process (outside NT) to push closed files to Spaces.
//   - Optional .ready marker and local SQLite spool catalog.
//
// WARNING:
//   NT versions/providers differ. You may need small API/namespace adjustments to compile in your environment.
//
// File format example (one line JSON per event, gzipped):
//   {"ts_recv_utc_ns":1700000000000000000,"type":"trade","schema":"nt_bridge_v1","data":{...}}

#region Using declarations
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Text;
using System.Threading;
using NinjaTrader.Cbi;
using NinjaTrader.Data;
using NinjaTrader.Gui;
using NinjaTrader.Gui.Chart;
using NinjaTrader.NinjaScript;
using NinjaTrader.NinjaScript.Strategies;
#endregion

namespace NinjaTrader.NinjaScript.Strategies
{
    public class LobsterCapNtBridgeStrategy : Strategy
    {
        // -----------------------------
        // User parameters (editable in NT UI)
        // -----------------------------
        [NinjaScriptProperty]
        [Display(Name = "SpoolDirectory", GroupName = "LOBSTER Bridge", Order = 1)]
        public string SpoolDirectory { get; set; }

        [NinjaScriptProperty]
        [Display(Name = "RotateHours", GroupName = "LOBSTER Bridge", Order = 2)]
        public int RotateHours { get; set; }

        [NinjaScriptProperty]
        [Display(Name = "FlushIntervalMs", GroupName = "LOBSTER Bridge", Order = 3)]
        public int FlushIntervalMs { get; set; }

        [NinjaScriptProperty]
        [Display(Name = "QueueCapacity", GroupName = "LOBSTER Bridge", Order = 4)]
        public int QueueCapacity { get; set; }

        [NinjaScriptProperty]
        [Display(Name = "AssumeFeedTimeUtcWhenUnspecified", GroupName = "LOBSTER Bridge", Order = 5)]
        public bool AssumeFeedTimeUtcWhenUnspecified { get; set; }

        [NinjaScriptProperty]
        [Display(Name = "EmitDepth", GroupName = "LOBSTER Bridge", Order = 6)]
        public bool EmitDepth { get; set; }

        [NinjaScriptProperty]
        [Display(Name = "EmitL1", GroupName = "LOBSTER Bridge", Order = 7)]
        public bool EmitL1 { get; set; }

        [NinjaScriptProperty]
        [Display(Name = "EmitTrades", GroupName = "LOBSTER Bridge", Order = 8)]
        public bool EmitTrades { get; set; }

        [NinjaScriptProperty]
        [Display(Name = "EmitReadyMarker", GroupName = "LOBSTER Bridge", Order = 9)]
        public bool EmitReadyMarker { get; set; }

        [NinjaScriptProperty]
        [Display(Name = "WriterIdleSleepMs", GroupName = "LOBSTER Bridge", Order = 10)]
        public int WriterIdleSleepMs { get; set; }

        // -----------------------------
        // Runtime bridge state
        // -----------------------------
        private readonly ConcurrentQueue<BridgeMsg> queue = new ConcurrentQueue<BridgeMsg>();
        private readonly AutoResetEvent queueSignal = new AutoResetEvent(false);
        private Thread writerThread;
        private volatile bool writerStopRequested;
        private volatile bool writerStarted;
        private volatile bool rotationRequested;
        private long droppedQueueRecords;
        private long emittedSeq;

        // Depth cache (optional future snapshot_ladder emission)
        // side -> position -> (price,size)
        private readonly object depthCacheLock = new object();
        private readonly Dictionary<int, DepthLevel> bidDepthByPos = new Dictionary<int, DepthLevel>();
        private readonly Dictionary<int, DepthLevel> askDepthByPos = new Dictionary<int, DepthLevel>();

        // Track last known inside quote from L1/depth callbacks (for convenience in trade records)
        private volatile double lastBestBid = double.NaN;
        private volatile double lastBestAsk = double.NaN;
        private volatile long lastBestBidSize = -1;
        private volatile long lastBestAskSize = -1;

        // Writer-owned file state (only touched by writer thread)
        private FileStream activeFs;
        private GZipStream activeGz;
        private StreamWriter activeSw;
        private string activeTmpPath;
        private string activeFinalPath;
        private DateTime blockStartUtc;
        private DateTime nextRotateUtc;
        private long recordsInBlock;
        private DateTime lastFlushUtc;
        private long fileOrdinal;

        // Simple metrics for status/meta records
        private long depthEventCount;
        private long l1EventCount;
        private long tradeEventCount;

        // -----------------------------
        // NT lifecycle
        // -----------------------------
        protected override void OnStateChange()
        {
            if (State == State.SetDefaults)
            {
                Name = "LobsterCapNtBridgeStrategy";
                Description = "LOBSTER-CAP bridge recorder: writes NT market events to rotating .jsonl.gz files.";
                Calculate = Calculate.OnEachTick;
                IsOverlay = true;
                IsUnmanaged = false;
                IsAdoptAccountPositionAware = false;
                EntriesPerDirection = 1;
                EntryHandling = EntryHandling.AllEntries;
                IsExitOnSessionCloseStrategy = false;
                IncludeTradeHistoryInBacktest = false;
                MaximumBarsLookBack = MaximumBarsLookBack.TwoHundredFiftySix;
                BarsRequiredToTrade = 0;

                SpoolDirectory = @"C:\\lobstercap_nt_spool";
                RotateHours = 8;
                FlushIntervalMs = 1000;
                QueueCapacity = 200000;
                AssumeFeedTimeUtcWhenUnspecified = true;
                EmitDepth = true;
                EmitL1 = true;
                EmitTrades = true;
                EmitReadyMarker = true;
                WriterIdleSleepMs = 5;
            }
            else if (State == State.Configure)
            {
                // No trading logic. Strategy is used as a data bridge attached to an instrument.
            }
            else if (State == State.DataLoaded)
            {
                EnsureSpoolDirectory();
                StartWriter();
                EnqueueMetaStart("strategy_data_loaded");
            }
            else if (State == State.Terminated)
            {
                try
                {
                    EnqueueStatus("terminated", "Strategy terminated");
                }
                catch { }
                StopWriter();
            }
        }

        // -----------------------------
        // NT market callbacks (event emission only; keep lightweight)
        // -----------------------------
        protected override void OnMarketData(MarketDataEventArgs e)
        {
            if (e == null || Instrument == null)
                return;

            DateTime tsRecvUtc = DateTime.UtcNow;
            DateTime tsEventUtc = NormalizeToUtc(e.Time);
            string instrument = Instrument.FullName ?? Instrument.MasterInstrument?.Name ?? "UNKNOWN";

            // L1 quote changes (best bid/ask) and trades all come through here.
            if (EmitL1 && (e.MarketDataType == MarketDataType.Bid || e.MarketDataType == MarketDataType.Ask))
            {
                if (e.MarketDataType == MarketDataType.Bid)
                {
                    lastBestBid = e.Price;
                    lastBestBidSize = (long)Math.Round(e.Volume);
                }
                else if (e.MarketDataType == MarketDataType.Ask)
                {
                    lastBestAsk = e.Price;
                    lastBestAskSize = (long)Math.Round(e.Volume);
                }

                l1EventCount++;
                var line = BuildL1Line(
                    tsRecvUtc,
                    instrument,
                    tsEventUtc,
                    e.MarketDataType.ToString(),
                    e.Price,
                    SafeLongFromDouble(e.Volume),
                    lastBestBid,
                    lastBestAsk,
                    lastBestBidSize,
                    lastBestAskSize,
                    NextSeq());
                EnqueueLine(line);
            }

            if (EmitTrades && e.MarketDataType == MarketDataType.Last)
            {
                tradeEventCount++;
                var line = BuildTradeLine(
                    tsRecvUtc,
                    instrument,
                    tsEventUtc,
                    e.Price,
                    SafeLongFromDouble(e.Volume),
                    InferAggressorSide(e.Price, lastBestBid, lastBestAsk),
                    lastBestBid,
                    lastBestAsk,
                    NextSeq());
                EnqueueLine(line);
            }

            // Optional: also emit other market data types if useful for later (settlement/openinterest/etc.)
            // You can add a generic l1_misc emitter here.
        }

        protected override void OnMarketDepth(MarketDepthEventArgs e)
        {
            if (!EmitDepth || e == null || Instrument == null)
                return;

            DateTime tsRecvUtc = DateTime.UtcNow;
            DateTime tsEventUtc = NormalizeToUtc(e.Time);
            string instrument = Instrument.FullName ?? Instrument.MasterInstrument?.Name ?? "UNKNOWN";

            // Maintain a local depth cache by side+position so you can later emit synthetic snapshots.
            // NOTE: This is not guaranteed to be a true provider snapshot on startup; it is a running cache.
            UpdateDepthCache(e);

            depthEventCount++;
            var line = BuildDepthLine(
                tsRecvUtc,
                instrument,
                tsEventUtc,
                e.MarketDataType.ToString(),
                e.Operation.ToString(),
                e.Position,
                e.Price,
                SafeLongFromDouble(e.Volume),
                e.MarketMaker,
                NextSeq());
            EnqueueLine(line);
        }

        protected override void OnBarUpdate()
        {
            // Intentionally empty. This script uses market event callbacks, not bars.
        }

        // -----------------------------
        // Writer thread / queue / rotation
        // -----------------------------
        private void StartWriter()
        {
            if (writerStarted)
                return;

            writerStopRequested = false;
            writerThread = new Thread(WriterLoop)
            {
                Name = "LobsterCapNtBridgeWriter",
                IsBackground = true
            };
            writerThread.Start();
            writerStarted = true;
        }

        private void StopWriter()
        {
            if (!writerStarted)
                return;

            writerStopRequested = true;
            queueSignal.Set();
            try
            {
                if (writerThread != null && writerThread.IsAlive)
                    writerThread.Join(TimeSpan.FromSeconds(10));
            }
            catch { }
            finally
            {
                writerStarted = false;
            }
        }

        private void WriterLoop()
        {
            try
            {
                OpenNewBlock(DateTime.UtcNow, "startup");

                while (!writerStopRequested || !queue.IsEmpty)
                {
                    DrainQueueBatch(maxItems: 5000);

                    DateTime nowUtc = DateTime.UtcNow;
                    if (rotationRequested || nowUtc >= nextRotateUtc)
                    {
                        rotationRequested = false;
                        RotateBlock("scheduled_or_requested");
                    }

                    if (activeSw != null && (nowUtc - lastFlushUtc).TotalMilliseconds >= FlushIntervalMs)
                    {
                        FlushActive();
                        lastFlushUtc = nowUtc;
                    }

                    if (queue.IsEmpty)
                        queueSignal.WaitOne(WriterIdleSleepMs);
                }

                // Final block close
                RotateBlock("shutdown", closeOnly: true);
            }
            catch (Exception ex)
            {
                // Last-resort logging to NT Output window
                try { Print("[LOBSTER Bridge] WriterLoop fatal: " + ex); } catch { }
                try { SafeDisposeWriters(); } catch { }
            }
        }

        private void DrainQueueBatch(int maxItems)
        {
            int n = 0;
            while (n < maxItems && queue.TryDequeue(out BridgeMsg msg))
            {
                n++;
                if (msg.Kind == BridgeMsgKind.Line)
                {
                    EnsureWriterOpen();
                    activeSw.WriteLine(msg.Line);
                    recordsInBlock++;
                }
                else if (msg.Kind == BridgeMsgKind.Rotate)
                {
                    rotationRequested = true;
                }
            }
        }

        private void EnsureWriterOpen()
        {
            if (activeSw == null)
                OpenNewBlock(DateTime.UtcNow, "ensure_open");
        }

        private void OpenNewBlock(DateTime nowUtc, string reason)
        {
            if (RotateHours <= 0)
                RotateHours = 8;

            // Align to RotateHours boundary in UTC (e.g., 00:00, 08:00, 16:00)
            blockStartUtc = AlignUtcBlockStart(nowUtc, RotateHours);
            nextRotateUtc = blockStartUtc.AddHours(RotateHours);
            recordsInBlock = 0;
            lastFlushUtc = DateTime.UtcNow;

            string instrumentTag = SanitizeForPath(Instrument != null ? (Instrument.FullName ?? Instrument.MasterInstrument?.Name ?? "UNKNOWN") : "UNKNOWN");
            string datePart = blockStartUtc.ToString("yyyyMMdd'T'HHmmss'Z'", CultureInfo.InvariantCulture);
            string fileNameBase = string.Format(CultureInfo.InvariantCulture,
                "source=nt8_rithmic_apex/symbol={0}/dt={1:yyyy-MM-dd}/{0}__{1}__{2:D6}",
                instrumentTag, blockStartUtc, fileOrdinal++);

            string finalPath = Path.Combine(SpoolDirectory, fileNameBase + ".jsonl.gz");
            string tmpPath = finalPath + ".partial";
            Directory.CreateDirectory(Path.GetDirectoryName(finalPath));

            activeTmpPath = tmpPath;
            activeFinalPath = finalPath;

            activeFs = new FileStream(activeTmpPath, FileMode.Create, FileAccess.Write, FileShare.Read, bufferSize: 1 << 20, useAsync: false);
            activeGz = new GZipStream(activeFs, CompressionLevel.Fastest, leaveOpen: false);
            activeSw = new StreamWriter(activeGz, new UTF8Encoding(false), bufferSize: 1 << 16);

            // Emit a file-open meta record as the first line of each block.
            string metaLine = BuildMetaStartLine(DateTime.UtcNow,
                Instrument != null ? (Instrument.FullName ?? Instrument.MasterInstrument?.Name ?? "UNKNOWN") : "UNKNOWN",
                blockStartUtc,
                nextRotateUtc,
                reason,
                RotateHours,
                FlushIntervalMs,
                QueueCapacity,
                NextSeq());
            activeSw.WriteLine(metaLine);
            recordsInBlock++;

            // Optional TODO: emit synthetic snapshot_ladder from current depth cache here.
            // EmitSnapshotLadderIfAvailable();
        }

        private void RotateBlock(string reason, bool closeOnly = false)
        {
            if (activeSw == null)
                return;

            // Emit block end marker before closing so downstream knows the file closed cleanly.
            string endLine = BuildMetaEndLine(DateTime.UtcNow,
                Instrument != null ? (Instrument.FullName ?? Instrument.MasterInstrument?.Name ?? "UNKNOWN") : "UNKNOWN",
                blockStartUtc,
                DateTime.UtcNow,
                reason,
                recordsInBlock,
                depthEventCount,
                l1EventCount,
                tradeEventCount,
                droppedQueueRecords,
                NextSeq());
            activeSw.WriteLine(endLine);
            recordsInBlock++;

            FlushActive();
            SafeDisposeWriters();

            try
            {
                if (File.Exists(activeFinalPath))
                    File.Delete(activeFinalPath);
                File.Move(activeTmpPath, activeFinalPath);

                if (EmitReadyMarker)
                    File.WriteAllText(activeFinalPath + ".ready", string.Empty);
            }
            catch (Exception ex)
            {
                try { Print("[LOBSTER Bridge] Rotate rename failed: " + ex); } catch { }
            }

            if (!closeOnly)
                OpenNewBlock(DateTime.UtcNow, reason + "_next");
        }

        private void FlushActive()
        {
            if (activeSw == null)
                return;

            try
            {
                activeSw.Flush();
                activeGz.Flush();
                activeFs.Flush(flushToDisk: true);
            }
            catch (Exception ex)
            {
                try { Print("[LOBSTER Bridge] Flush failed: " + ex); } catch { }
            }
        }

        private void SafeDisposeWriters()
        {
            try { if (activeSw != null) activeSw.Dispose(); } catch { }
            try { if (activeGz != null) activeGz.Dispose(); } catch { }
            try { if (activeFs != null) activeFs.Dispose(); } catch { }
            activeSw = null;
            activeGz = null;
            activeFs = null;
        }

        private void EnqueueLine(string jsonLine)
        {
            // Soft-bounded queue: if queue grows beyond capacity, drop and count.
            // (Alternative: block callbacks, but dropping is safer than freezing NT.)
            if (queue.Count >= QueueCapacity)
            {
                Interlocked.Increment(ref droppedQueueRecords);
                return;
            }

            queue.Enqueue(BridgeMsg.Line(jsonLine));
            queueSignal.Set();
        }

        private void RequestRotate()
        {
            queue.Enqueue(BridgeMsg.Rotate());
            queueSignal.Set();
        }

        // -----------------------------
        // Public/manual controls (call from UI or custom hotkey integration if desired)
        // -----------------------------
        // You can expose this through a button in an AddOn shell later.
        public void ManualRotateNow()
        {
            RequestRotate();
        }

        // -----------------------------
        // Meta/status helpers
        // -----------------------------
        private void EnqueueMetaStart(string reason)
        {
            string instrument = Instrument != null ? (Instrument.FullName ?? Instrument.MasterInstrument?.Name ?? "UNKNOWN") : "UNKNOWN";
            var line = BuildStatusLine(DateTime.UtcNow, instrument, "bridge_started", reason, NextSeq());
            EnqueueLine(line);
        }

        private void EnqueueStatus(string status, string message)
        {
            string instrument = Instrument != null ? (Instrument.FullName ?? Instrument.MasterInstrument?.Name ?? "UNKNOWN") : "UNKNOWN";
            var line = BuildStatusLine(DateTime.UtcNow, instrument, status, message, NextSeq());
            EnqueueLine(line);
        }

        // -----------------------------
        // Timestamp handling
        // -----------------------------
        private DateTime NormalizeToUtc(DateTime dt)
        {
            // NT/provider timestamps may be unspecified/local/exchange-context depending on setup.
            // This helper enforces an explicit UTC output policy.
            if (dt.Kind == DateTimeKind.Utc)
                return dt;

            if (dt.Kind == DateTimeKind.Local)
                return dt.ToUniversalTime();

            // Unspecified: choose policy.
            if (AssumeFeedTimeUtcWhenUnspecified)
                return DateTime.SpecifyKind(dt, DateTimeKind.Utc);

            return DateTime.SpecifyKind(dt, DateTimeKind.Local).ToUniversalTime();
        }

        private static long ToUnixNs(DateTime utc)
        {
            if (utc.Kind != DateTimeKind.Utc)
                utc = utc.ToUniversalTime();
            long ticksSinceUnixEpoch = utc.Ticks - 621355968000000000L; // .NET ticks since 1970-01-01
            return ticksSinceUnixEpoch * 100L;
        }

        // -----------------------------
        // Depth cache maintenance (for future synthetic snapshots)
        // -----------------------------
        private void UpdateDepthCache(MarketDepthEventArgs e)
        {
            lock (depthCacheLock)
            {
                Dictionary<int, DepthLevel> sideMap = (e.MarketDataType == MarketDataType.Bid) ? bidDepthByPos : askDepthByPos;

                if (e.Operation == Operation.Remove)
                {
                    sideMap.Remove(e.Position);
                }
                else
                {
                    sideMap[e.Position] = new DepthLevel
                    {
                        Position = e.Position,
                        Price = e.Price,
                        Size = SafeLongFromDouble(e.Volume),
                        MarketMaker = e.MarketMaker ?? string.Empty
                    };
                }

                // Refresh top-of-book convenience values from pos 0 if available
                if (bidDepthByPos.TryGetValue(0, out DepthLevel bid0))
                {
                    lastBestBid = bid0.Price;
                    lastBestBidSize = bid0.Size;
                }
                if (askDepthByPos.TryGetValue(0, out DepthLevel ask0))
                {
                    lastBestAsk = ask0.Price;
                    lastBestAskSize = ask0.Size;
                }
            }
        }

        // -----------------------------
        // JSON line builders (manual, dependency-light)
        // -----------------------------
        private string BuildTradeLine(DateTime tsRecvUtc, string instrument, DateTime tsEventUtc, double price, long size, string aggressorSide, double bestBid, double bestAsk, long seq)
        {
            var sb = new StringBuilder(512);
            AppendEnvelopeOpen(sb, tsRecvUtc, "trade");
            sb.Append("\"data\":{");
            AppendJsonProp(sb, "instrument", instrument, true);
            AppendJsonProp(sb, "ts_event_utc", IsoUtc(tsEventUtc), false);
            AppendJsonProp(sb, "seq", seq, false);
            AppendJsonProp(sb, "price", price, false);
            AppendJsonProp(sb, "size", size, false);
            AppendJsonProp(sb, "aggressor_side", aggressorSide, false);
            AppendJsonProp(sb, "best_bid", bestBid, false, allowNaN: true);
            AppendJsonProp(sb, "best_ask", bestAsk, false, allowNaN: true);
            sb.Append("}}");
            return sb.ToString();
        }

        private string BuildL1Line(DateTime tsRecvUtc, string instrument, DateTime tsEventUtc, string mdType, double price, long size, double bestBid, double bestAsk, long bestBidSize, long bestAskSize, long seq)
        {
            var sb = new StringBuilder(512);
            AppendEnvelopeOpen(sb, tsRecvUtc, "l1");
            sb.Append("\"data\":{");
            AppendJsonProp(sb, "instrument", instrument, true);
            AppendJsonProp(sb, "ts_event_utc", IsoUtc(tsEventUtc), false);
            AppendJsonProp(sb, "seq", seq, false);
            AppendJsonProp(sb, "md_type", mdType, false);
            AppendJsonProp(sb, "price", price, false);
            AppendJsonProp(sb, "size", size, false);
            AppendJsonProp(sb, "best_bid", bestBid, false, allowNaN: true);
            AppendJsonProp(sb, "best_ask", bestAsk, false, allowNaN: true);
            AppendJsonProp(sb, "best_bid_size", bestBidSize, false);
            AppendJsonProp(sb, "best_ask_size", bestAskSize, false);
            sb.Append("}}");
            return sb.ToString();
        }

        private string BuildDepthLine(DateTime tsRecvUtc, string instrument, DateTime tsEventUtc, string side, string op, int pos, double price, long size, string marketMaker, long seq)
        {
            var sb = new StringBuilder(640);
            AppendEnvelopeOpen(sb, tsRecvUtc, "depth");
            sb.Append("\"data\":{");
            AppendJsonProp(sb, "instrument", instrument, true);
            AppendJsonProp(sb, "ts_event_utc", IsoUtc(tsEventUtc), false);
            AppendJsonProp(sb, "seq", seq, false);
            AppendJsonProp(sb, "side", side, false);
            AppendJsonProp(sb, "op", op, false);
            AppendJsonProp(sb, "position", pos, false);
            AppendJsonProp(sb, "price", price, false);
            AppendJsonProp(sb, "size", size, false);
            AppendJsonProp(sb, "market_maker", marketMaker ?? string.Empty, false);
            sb.Append("}}");
            return sb.ToString();
        }

        private string BuildStatusLine(DateTime tsRecvUtc, string instrument, string status, string message, long seq)
        {
            var sb = new StringBuilder(512);
            AppendEnvelopeOpen(sb, tsRecvUtc, "status");
            sb.Append("\"data\":{");
            AppendJsonProp(sb, "instrument", instrument, true);
            AppendJsonProp(sb, "ts_event_utc", IsoUtc(tsRecvUtc), false);
            AppendJsonProp(sb, "seq", seq, false);
            AppendJsonProp(sb, "status", status, false);
            AppendJsonProp(sb, "message", message ?? string.Empty, false);
            sb.Append("}}");
            return sb.ToString();
        }

        private string BuildMetaStartLine(DateTime tsRecvUtc, string instrument, DateTime blockStart, DateTime blockEnd, string reason, int rotateHours, int flushMs, int queueCap, long seq)
        {
            var sb = new StringBuilder(768);
            AppendEnvelopeOpen(sb, tsRecvUtc, "meta_start");
            sb.Append("\"data\":{");
            AppendJsonProp(sb, "instrument", instrument, true);
            AppendJsonProp(sb, "ts_event_utc", IsoUtc(tsRecvUtc), false);
            AppendJsonProp(sb, "seq", seq, false);
            AppendJsonProp(sb, "schema_version", "nt_bridge_v1", false);
            AppendJsonProp(sb, "source", "nt8_rithmic_apex", false);
            AppendJsonProp(sb, "block_start_utc", IsoUtc(blockStart), false);
            AppendJsonProp(sb, "block_end_target_utc", IsoUtc(blockEnd), false);
            AppendJsonProp(sb, "rotate_hours", rotateHours, false);
            AppendJsonProp(sb, "flush_interval_ms", flushMs, false);
            AppendJsonProp(sb, "queue_capacity", queueCap, false);
            AppendJsonProp(sb, "assume_feed_time_utc_unspecified", AssumeFeedTimeUtcWhenUnspecified, false);
            AppendJsonProp(sb, "reason", reason, false);
            sb.Append("}}");
            return sb.ToString();
        }

        private string BuildMetaEndLine(DateTime tsRecvUtc, string instrument, DateTime blockStart, DateTime blockClosed, string reason, long records, long depthCount, long l1Count, long tradeCount, long dropped, long seq)
        {
            var sb = new StringBuilder(896);
            AppendEnvelopeOpen(sb, tsRecvUtc, "meta_end");
            sb.Append("\"data\":{");
            AppendJsonProp(sb, "instrument", instrument, true);
            AppendJsonProp(sb, "ts_event_utc", IsoUtc(tsRecvUtc), false);
            AppendJsonProp(sb, "seq", seq, false);
            AppendJsonProp(sb, "schema_version", "nt_bridge_v1", false);
            AppendJsonProp(sb, "source", "nt8_rithmic_apex", false);
            AppendJsonProp(sb, "block_start_utc", IsoUtc(blockStart), false);
            AppendJsonProp(sb, "block_closed_utc", IsoUtc(blockClosed), false);
            AppendJsonProp(sb, "reason", reason, false);
            AppendJsonProp(sb, "records_in_block", records, false);
            AppendJsonProp(sb, "depth_events_total", depthCount, false);
            AppendJsonProp(sb, "l1_events_total", l1Count, false);
            AppendJsonProp(sb, "trade_events_total", tradeCount, false);
            AppendJsonProp(sb, "dropped_queue_records_total", dropped, false);
            sb.Append("}}");
            return sb.ToString();
        }

        private void AppendEnvelopeOpen(StringBuilder sb, DateTime tsRecvUtc, string type)
        {
            sb.Append('{');
            AppendJsonProp(sb, "schema", "nt_bridge_v1", true);
            AppendJsonProp(sb, "ts_recv_utc_ns", ToUnixNs(tsRecvUtc), false);
            AppendJsonProp(sb, "type", type, false);
        }

        private static void AppendJsonProp(StringBuilder sb, string key, string value, bool first)
        {
            if (!first) sb.Append(',');
            sb.Append('"').Append(EscapeJson(key)).Append("\":");
            if (value == null)
                sb.Append("null");
            else
                sb.Append('"').Append(EscapeJson(value)).Append('"');
        }

        private static void AppendJsonProp(StringBuilder sb, string key, bool value, bool first)
        {
            if (!first) sb.Append(',');
            sb.Append('"').Append(EscapeJson(key)).Append("\":").Append(value ? "true" : "false");
        }

        private static void AppendJsonProp(StringBuilder sb, string key, long value, bool first)
        {
            if (!first) sb.Append(',');
            sb.Append('"').Append(EscapeJson(key)).Append("\":").Append(value.ToString(CultureInfo.InvariantCulture));
        }

        private static void AppendJsonProp(StringBuilder sb, string key, int value, bool first)
        {
            if (!first) sb.Append(',');
            sb.Append('"').Append(EscapeJson(key)).Append("\":").Append(value.ToString(CultureInfo.InvariantCulture));
        }

        private static void AppendJsonProp(StringBuilder sb, string key, double value, bool first, bool allowNaN = false)
        {
            if (!first) sb.Append(',');
            sb.Append('"').Append(EscapeJson(key)).Append("\":");

            if (double.IsNaN(value) || double.IsInfinity(value))
            {
                if (allowNaN)
                    sb.Append("null");
                else
                    sb.Append('0');
                return;
            }

            sb.Append(value.ToString("G17", CultureInfo.InvariantCulture));
        }

        private static string EscapeJson(string s)
        {
            if (string.IsNullOrEmpty(s))
                return s ?? string.Empty;

            StringBuilder sb = null;
            for (int i = 0; i < s.Length; i++)
            {
                char c = s[i];
                bool needsEscape = c == '"' || c == '\\' || c < 0x20;
                if (!needsEscape) continue;

                if (sb == null)
                {
                    sb = new StringBuilder(s.Length + 16);
                    if (i > 0) sb.Append(s, 0, i);
                }

                switch (c)
                {
                    case '"': sb.Append("\\\""); break;
                    case '\\': sb.Append("\\\\"); break;
                    case '\b': sb.Append("\\b"); break;
                    case '\f': sb.Append("\\f"); break;
                    case '\n': sb.Append("\\n"); break;
                    case '\r': sb.Append("\\r"); break;
                    case '\t': sb.Append("\\t"); break;
                    default:
                        sb.Append("\\u").Append(((int)c).ToString("x4", CultureInfo.InvariantCulture));
                        break;
                }
            }

            if (sb == null)
                return s;

            // append remainder (characters after first escaped char continue in loop?)
            // Since loop appends only escapes, rebuild properly:
            sb.Clear();
            for (int i = 0; i < s.Length; i++)
            {
                char c = s[i];
                switch (c)
                {
                    case '"': sb.Append("\\\""); break;
                    case '\\': sb.Append("\\\\"); break;
                    case '\b': sb.Append("\\b"); break;
                    case '\f': sb.Append("\\f"); break;
                    case '\n': sb.Append("\\n"); break;
                    case '\r': sb.Append("\\r"); break;
                    case '\t': sb.Append("\\t"); break;
                    default:
                        if (c < 0x20)
                            sb.Append("\\u").Append(((int)c).ToString("x4", CultureInfo.InvariantCulture));
                        else
                            sb.Append(c);
                        break;
                }
            }
            return sb.ToString();
        }

        private static string IsoUtc(DateTime utc)
        {
            if (utc.Kind != DateTimeKind.Utc)
                utc = utc.ToUniversalTime();
            // Up to ticks precision in ISO; nanoseconds already available in ts_recv_utc_ns.
            return utc.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'", CultureInfo.InvariantCulture);
        }

        // -----------------------------
        // Misc helpers
        // -----------------------------
        private static DateTime AlignUtcBlockStart(DateTime utcNow, int hours)
        {
            if (utcNow.Kind != DateTimeKind.Utc)
                utcNow = utcNow.ToUniversalTime();
            int alignedHour = (utcNow.Hour / hours) * hours;
            return new DateTime(utcNow.Year, utcNow.Month, utcNow.Day, alignedHour, 0, 0, DateTimeKind.Utc);
        }

        private static string SanitizeForPath(string s)
        {
            if (string.IsNullOrWhiteSpace(s))
                return "UNKNOWN";
            foreach (char c in Path.GetInvalidFileNameChars())
                s = s.Replace(c, '_');
            s = s.Replace(' ', '_').Replace('/', '_').Replace('\\', '_').Replace(':', '_');
            return s;
        }

        private static long SafeLongFromDouble(double x)
        {
            if (double.IsNaN(x) || double.IsInfinity(x))
                return 0;
            try { return Convert.ToInt64(Math.Round(x), CultureInfo.InvariantCulture); }
            catch { return 0; }
        }

        private long NextSeq()
        {
            return Interlocked.Increment(ref emittedSeq);
        }

        private static string InferAggressorSide(double tradePrice, double bestBid, double bestAsk)
        {
            // Heuristic only. Some providers/platforms may not expose aggressor side directly.
            if (!double.IsNaN(bestAsk) && tradePrice >= bestAsk) return "buy";
            if (!double.IsNaN(bestBid) && tradePrice <= bestBid) return "sell";
            return "unknown";
        }

        private void EnsureSpoolDirectory()
        {
            if (string.IsNullOrWhiteSpace(SpoolDirectory))
                SpoolDirectory = @"C:\\lobstercap_nt_spool";
            Directory.CreateDirectory(SpoolDirectory);
        }

        // -----------------------------
        // Internal structs
        // -----------------------------
        private enum BridgeMsgKind
        {
            Line,
            Rotate
        }

        private struct BridgeMsg
        {
            public BridgeMsgKind Kind;
            public string Line;

            public static BridgeMsg Line(string line)
            {
                return new BridgeMsg { Kind = BridgeMsgKind.Line, Line = line };
            }

            public static BridgeMsg Rotate()
            {
                return new BridgeMsg { Kind = BridgeMsgKind.Rotate, Line = null };
            }
        }

        private sealed class DepthLevel
        {
            public int Position;
            public double Price;
            public long Size;
            public string MarketMaker;
        }
    }
}
