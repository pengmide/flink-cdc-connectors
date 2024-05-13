/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mysql.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import com.ververica.cdc.connectors.mysql.source.assigners.MySqlHybridSplitAssigner;
import com.ververica.cdc.connectors.mysql.source.assigners.MySqlSplitAssigner;
import com.ververica.cdc.connectors.mysql.source.assigners.state.PendingSplitsState;
import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import com.ververica.cdc.connectors.mysql.source.events.BinlogSplitMetaEvent;
import com.ververica.cdc.connectors.mysql.source.events.BinlogSplitMetaRequestEvent;
import com.ververica.cdc.connectors.mysql.source.events.FinishedSnapshotSplitsAckEvent;
import com.ververica.cdc.connectors.mysql.source.events.FinishedSnapshotSplitsReportEvent;
import com.ververica.cdc.connectors.mysql.source.events.FinishedSnapshotSplitsRequestEvent;
import com.ververica.cdc.connectors.mysql.source.events.LatestFinishedSplitsSizeEvent;
import com.ververica.cdc.connectors.mysql.source.events.LatestFinishedSplitsSizeRequestEvent;
import com.ververica.cdc.connectors.mysql.source.events.SuspendBinlogReaderAckEvent;
import com.ververica.cdc.connectors.mysql.source.events.SuspendBinlogReaderEvent;
import com.ververica.cdc.connectors.mysql.source.events.WakeupReaderEvent;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.ververica.cdc.connectors.mysql.source.split.FinishedSnapshotSplitInfo;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.mysql.source.assigners.AssignerStatus.isAssigning;
import static com.ververica.cdc.connectors.mysql.source.assigners.AssignerStatus.isAssigningFinished;
import static com.ververica.cdc.connectors.mysql.source.assigners.AssignerStatus.isSuspended;

// splitEnumerator的作用:
// 1. 处理sourceReader的split请求
// 2. 将split分配给sourceReader
//
// MySqlSourceReader 启动时会向 MySqlSourceEnumerator 发送请求 RequestSplitEvent 事件，根据返回的切片范围读取区间数据
/**
 *
 * A MySQL CDC source enumerator that enumerates receive the split request and assign the split to
 * source readers.
 */
// 继承SplitEnumerator,并重写其方法
@Internal
public class MySqlSourceEnumerator implements SplitEnumerator<MySqlSplit, PendingSplitsState> {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlSourceEnumerator.class);
    private static final long CHECK_EVENT_INTERVAL = 30_000L;

    private final SplitEnumeratorContext<MySqlSplit> context;
    private final MySqlSourceConfig sourceConfig;
    private final MySqlSplitAssigner splitAssigner;

    // using TreeSet to prefer assigning binlog split to task-0 for easier debug
    private final TreeSet<Integer> readersAwaitingSplit;
    private List<List<FinishedSnapshotSplitInfo>> binlogSplitMeta;
    private boolean binlogReaderIsSuspended = false;

    public MySqlSourceEnumerator(
            SplitEnumeratorContext<MySqlSplit> context,
            MySqlSourceConfig sourceConfig,
            MySqlSplitAssigner splitAssigner) {

        // source.createEnumerator传入的context对象
        this.context = context;
        this.sourceConfig = sourceConfig;
        this.splitAssigner = splitAssigner;
        this.readersAwaitingSplit = new TreeSet<>();

        // when restored from state, if the split assigner is assigning snapshot
        // splits or has already assigned all splits, send wakeup event to
        // SourceReader, SourceReader can omit the event based on its own status.
        if (isAssigning(splitAssigner.getAssignerStatus())
                || isAssigningFinished(splitAssigner.getAssignerStatus())) {
            binlogReaderIsSuspended = true;
        }
    }

    @Override
    public void start() {
        // 调用splitAssigner的open方法,可以具体看看每个splitAssigner的实现
        splitAssigner.open();
        suspendBinlogReaderIfNeed();
        // 注册一个Callable,定期调用,主要的作用就是当reader出现通信失败或者故障重启之后,检查是否有错过的通知时间,不是终点
        wakeupBinlogReaderIfNeed();
        this.context.callAsync(
                this::getRegisteredReader,
                this::syncWithReaders,
                CHECK_EVENT_INTERVAL,
                CHECK_EVENT_INTERVAL);
    }

    // 处理split的请求,当有具体subtask id的reader调用SourceReaderContext.sendSplitRequest()方法时，将调用此方法。
    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        // 将请求的taskId放入等待列表
        // 将reader所属的subtaskId存储到TreeSet, 在处理binlog split时优先分配个task-0
        readersAwaitingSplit.add(subtaskId);
        // 对等待列表的subtask进行分配
        assignSplits();
    }

    // 将split添加至splitEnumerator,只有在最后一个成功的checkpoint之后,分配的spilt才会出现此情况,说明需要重新处理.
    @Override
    public void addSplitsBack(List<MySqlSplit> splits, int subtaskId) {
        LOG.debug("MySQL Source Enumerator adds splits back: {}", splits);
        splitAssigner.addSplits(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        // send SuspendBinlogReaderEvent to source reader if the assigner's status is
        // suspended
        if (isSuspended(splitAssigner.getAssignerStatus())) {
            context.sendEventToSourceReader(subtaskId, new SuspendBinlogReaderEvent());
        }
    }

    // 处理sourceReader的自定义event
    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        // sourceReader发送给splitEnumerator的SourceEvent通知snapshot的split已经读取完成,binlog的位置是一致的
        if (sourceEvent instanceof FinishedSnapshotSplitsReportEvent) {
            LOG.info(
                    "The enumerator receives finished split offsets {} from subtask {}.",
                    sourceEvent,
                    subtaskId);
            FinishedSnapshotSplitsReportEvent reportEvent =
                    (FinishedSnapshotSplitsReportEvent) sourceEvent;
            Map<String, BinlogOffset> finishedOffsets = reportEvent.getFinishedOffsets();

            // 上面splitAssigner介绍过
            splitAssigner.onFinishedSplits(finishedOffsets);

            wakeupBinlogReaderIfNeed();

            // 返回ACK事件返回给reader的表示已经确认了snapshot
            // send acknowledge event
            FinishedSnapshotSplitsAckEvent ackEvent =
                    new FinishedSnapshotSplitsAckEvent(new ArrayList<>(finishedOffsets.keySet()));
            context.sendEventToSourceReader(subtaskId, ackEvent);
        // sourceReader发送给splitEnumerator的SourceEvent用来拉取binlog元数据，也就是发送BinlogSplitMetaEvent
        } else if (sourceEvent instanceof BinlogSplitMetaRequestEvent) {
            LOG.debug(
                    "The enumerator receives request for binlog split meta from subtask {}.",
                    subtaskId);
            // 发送binlog meta
            sendBinlogMeta(subtaskId, (BinlogSplitMetaRequestEvent) sourceEvent);
        } else if (sourceEvent instanceof SuspendBinlogReaderAckEvent) {
            LOG.info(
                    "The enumerator receives event that the binlog split reader has been suspended from subtask {}. ",
                    subtaskId);
            handleSuspendBinlogReaderAckEvent(subtaskId);
        } else if (sourceEvent instanceof LatestFinishedSplitsSizeRequestEvent) {
            handleLatestFinishedSplitSizeRequest(subtaskId);
        }
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) {
        return splitAssigner.snapshotState(checkpointId);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        splitAssigner.notifyCheckpointComplete(checkpointId);
        // binlog split may be available after checkpoint complete
        assignSplits();
    }

    @Override
    public void close() {
        LOG.info("Closing enumerator...");
        splitAssigner.close();
    }

    // ------------------------------------------------------------------------------------------

    // 为等待列表的subtask分配切片
    private void assignSplits() {
        // treeSet返回的iter是排好序的,即按照subtask id顺序依次处理
        final Iterator<Integer> awaitingReader = readersAwaitingSplit.iterator();

        while (awaitingReader.hasNext()) {
            int nextAwaiting = awaitingReader.next();
            // 如果reader再次请求的split在此期间失败，则将其从等待列表中删除
            // if the reader that requested another split has failed in the meantime, remove
            // it from the list of waiting readers
            if (!context.registeredReaders().containsKey(nextAwaiting)) {
                awaitingReader.remove();
                continue;
            }

            // 由 MySqlSplitAssigner 分配切片
            Optional<MySqlSplit> split = splitAssigner.getNext();
            if (split.isPresent()) {
                final MySqlSplit mySqlSplit = split.get();
                // 为subtask分配split
                // 发送AddSplitEvent, 为 Reader 返回切片信息
                context.assignSplit(mySqlSplit, nextAwaiting);
                awaitingReader.remove();
                LOG.info("Assign split {} to subtask {}", mySqlSplit, nextAwaiting);
            } else {
                // 前面splitAssigner中会分配空值,在这里被过滤掉
                // there is no available splits by now, skip assigning
                wakeupBinlogReaderIfNeed();
                break;
            }
        }
    }

    private int[] getRegisteredReader() {
        return this.context.registeredReaders().keySet().stream()
                .mapToInt(Integer::intValue)
                .toArray();
    }

    // 启动周期调度线程, 要求 SourceReader 向 SourceEnumerator 发送已完成但未发送ACK事件的切片信息
    private void syncWithReaders(int[] subtaskIds, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to list obtain registered readers due to:", t);
        }
        // when the SourceEnumerator restores or the communication failed between
        // SourceEnumerator and SourceReader, it may missed some notification event.
        // tell all SourceReader(s) to report there finished but unacked splits.
        if (splitAssigner.waitingForFinishedSplits()) {
            for (int subtaskId : subtaskIds) {
                context.sendEventToSourceReader(
                        subtaskId, new FinishedSnapshotSplitsRequestEvent());
            }
        }

        suspendBinlogReaderIfNeed();
        wakeupBinlogReaderIfNeed();
    }

    private void suspendBinlogReaderIfNeed() {
        if (isSuspended(splitAssigner.getAssignerStatus())) {
            for (int subtaskId : getRegisteredReader()) {
                context.sendEventToSourceReader(subtaskId, new SuspendBinlogReaderEvent());
            }
            binlogReaderIsSuspended = true;
        }
    }

    private void wakeupBinlogReaderIfNeed() {
        if (isAssigningFinished(splitAssigner.getAssignerStatus()) && binlogReaderIsSuspended) {
            for (int subtaskId : getRegisteredReader()) {
                context.sendEventToSourceReader(
                        subtaskId,
                        new WakeupReaderEvent(WakeupReaderEvent.WakeUpTarget.BINLOG_READER));
            }
            binlogReaderIsSuspended = false;
        }
    }

    // 发送给binlog meta event到reader
    private void sendBinlogMeta(int subTask, BinlogSplitMetaRequestEvent requestEvent) {
        // 如果binlog meta ==null 则进行meta的初始化操作
        // initialize once
        if (binlogSplitMeta == null) {
            final List<FinishedSnapshotSplitInfo> finishedSnapshotSplitInfos =
                    splitAssigner.getFinishedSplitInfos();
            if (finishedSnapshotSplitInfos.isEmpty()) {
                LOG.error(
                        "The assigner offer empty finished split information, this should not happen");
                throw new FlinkRuntimeException(
                        "The assigner offer empty finished split information, this should not happen");
            }
            binlogSplitMeta =
                    Lists.partition(
                            finishedSnapshotSplitInfos, sourceConfig.getSplitMetaGroupSize());
        }
        final int requestMetaGroupId = requestEvent.getRequestMetaGroupId();

        if (binlogSplitMeta.size() > requestMetaGroupId) {
            // 获取对应的FinishedSnapshotSplitInfo列表,并将其封序列化,生成meta event
            List<FinishedSnapshotSplitInfo> metaToSend = binlogSplitMeta.get(requestMetaGroupId);
            BinlogSplitMetaEvent metadataEvent =
                    new BinlogSplitMetaEvent(
                            requestEvent.getSplitId(),
                            requestMetaGroupId,
                            metaToSend.stream()
                                    .map(FinishedSnapshotSplitInfo::serialize)
                                    .collect(Collectors.toList()));
            // 将生成的meta event 发送给reader
            context.sendEventToSourceReader(subTask, metadataEvent);
        } else {
            LOG.error(
                    "Received invalid request meta group id {}, the valid meta group id range is [0, {}]",
                    requestMetaGroupId,
                    binlogSplitMeta.size() - 1);
        }
    }

    private void handleSuspendBinlogReaderAckEvent(int subTask) {
        LOG.info(
                "Received event that the binlog split reader has been suspended from subtask {}. ",
                subTask);
        splitAssigner.wakeup();
        if (splitAssigner instanceof MySqlHybridSplitAssigner) {
            for (int subtaskId : this.getRegisteredReader()) {
                context.sendEventToSourceReader(
                        subtaskId,
                        new WakeupReaderEvent(WakeupReaderEvent.WakeUpTarget.SNAPSHOT_READER));
            }
        }
    }

    private void handleLatestFinishedSplitSizeRequest(int subTask) {
        if (splitAssigner instanceof MySqlHybridSplitAssigner) {
            context.sendEventToSourceReader(
                    subTask,
                    new LatestFinishedSplitsSizeEvent(
                            splitAssigner.getFinishedSplitInfos().size()));
        }
    }
}
