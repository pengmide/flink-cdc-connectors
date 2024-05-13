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

package com.ververica.cdc.connectors.mysql.source.reader;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.util.FlinkRuntimeException;

import com.ververica.cdc.connectors.mysql.debezium.DebeziumUtils;
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
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplitState;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSnapshotSplitState;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplitState;
import com.ververica.cdc.connectors.mysql.source.split.SourceRecords;
import com.ververica.cdc.connectors.mysql.source.utils.TableDiscoveryUtils;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.mysql.source.events.WakeupReaderEvent.WakeUpTarget.SNAPSHOT_READER;
import static com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit.toNormalBinlogSplit;
import static com.ververica.cdc.connectors.mysql.source.split.MySqlBinlogSplit.toSuspendedBinlogSplit;
import static com.ververica.cdc.connectors.mysql.source.utils.ChunkUtils.getNextMetaGroupId;

// SourceOperator 集成了SourceReader，通过OperatorEventGateway 和 SourceCoordinator 进行交互。
// 1. SourceOperator 在初始化时，通过 MySqlParallelSource 创建 MySqlSourceReader。
//    MySqlSourceReader 通过 SingleThreadFetcherManager 创建Fetcher拉取分片数据，数据以 MySqlRecords 格式写入到 elementsQueue。
// 2. 将创建的 MySqlSourceReader 以事件的形式传递给 SourceCoordinator 进行注册。
//    SourceCoordinator 接收到注册事件后，将reader 地址及索引进行保存。
// 3. MySqlSourceReader 启动后会向 MySqlSourceEnumerator 发送请求分片事件，从而收集分配的切片数据
// 4. SourceOperator 初始化完毕后，调用 emitNext 由 SourceReaderBase 从 elementsQueue 获取数据集合并下发给 MySqlRecordEmitter
/** The source reader for MySQL source splits. */
public class MySqlSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                SourceRecords, T, MySqlSplit, MySqlSplitState> {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSourceReader.class);

    private final MySqlSourceConfig sourceConfig;
    private final Map<String, MySqlSnapshotSplit> finishedUnackedSplits;
    private final Map<String, MySqlBinlogSplit> uncompletedBinlogSplits;
    private final int subtaskId;
    private final MySqlSourceReaderContext mySqlSourceReaderContext;
    private MySqlBinlogSplit suspendedBinlogSplit;

    public MySqlSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<SourceRecords>> elementQueue,
            Supplier<MySqlSplitReader> splitReaderSupplier,
            RecordEmitter<SourceRecords, T, MySqlSplitState> recordEmitter,
            Configuration config,
            MySqlSourceReaderContext context,
            MySqlSourceConfig sourceConfig) {
        super(
                elementQueue,
                // 一个单线程的fetcher管理器,做一些读取操作
                // 简单描述一下流程:
                // SingleThreadFetcherManager.createSplitFetcher构建一个SplitFetcher(实现了Runnable),
                // 在SplitFetcher中会构建一个fetcherTask,SplitFetcher.run方法中,循环调用this.runOnce(),
                // this.runOnce()会持续调用fetcherTask.run()读取数据,
                // run()会调用MySqlSplitReader.fetch方法,返回reader读取的数据,
                // 并将数据放入到elementQueue中(只要涉及都多线程的代码,都比较晦涩难懂)
                //【这里是重点]，实际上当系统调用MySqlSourceReader.addSplits的时候就开始启动任务读取数据了
                // 调用链如下:
                // 1. sourceReader中创建的fetcherManager(new SingleThreadFetcherManager),存入父类成员变量中
                // 2. 调用父类的start方法,启动fetcherManager
                // 3. 调用我们传入的fetcherManager的addSplits方法
                // 4. 调用fetcherManager的addSplits方法时,子类没有覆写父类方法,直接进入父类方法,
                //    这里直接进入父类的splits方法,如果fetcher没有启动,则创建fetcher(一个runnable对象),然后提交到线程池执行任务
                new SingleThreadFetcherManager<>(elementQueue, splitReaderSupplier::get),
                recordEmitter,
                config,
                context.getSourceReaderContext());
        this.sourceConfig = sourceConfig;
        this.finishedUnackedSplits = new HashMap<>();
        this.uncompletedBinlogSplits = new HashMap<>();
        this.subtaskId = context.getSourceReaderContext().getIndexOfSubtask();
        this.mySqlSourceReaderContext = context;
        this.suspendedBinlogSplit = null;
    }

    // 启动reader
    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    // 当reader分配到新的split的时候,会初始化一个split的state
    @Override
    protected MySqlSplitState initializedState(MySqlSplit split) {
        if (split.isSnapshotSplit()) {
            return new MySqlSnapshotSplitState(split.asSnapshotSplit());
        } else {
            return new MySqlBinlogSplitState(split.asBinlogSplit());
        }
    }

    // 容错相关
    @Override
    public List<MySqlSplit> snapshotState(long checkpointId) {
        List<MySqlSplit> stateSplits = super.snapshotState(checkpointId);

        // unfinished splits
        List<MySqlSplit> unfinishedSplits =
                stateSplits.stream()
                        .filter(split -> !finishedUnackedSplits.containsKey(split.splitId()))
                        .collect(Collectors.toList());

        // add finished snapshot splits that didn't receive ack yet
        unfinishedSplits.addAll(finishedUnackedSplits.values());

        // add binlog splits who are uncompleted
        unfinishedSplits.addAll(uncompletedBinlogSplits.values());

        // add suspended BinlogSplit
        if (suspendedBinlogSplit != null) {
            unfinishedSplits.add(suspendedBinlogSplit);
        }

        logCurrentBinlogOffsets(unfinishedSplits, checkpointId);

        return unfinishedSplits;
    }

    // 清理处理已完成的split状态,非重点
    @Override
    protected void onSplitFinished(Map<String, MySqlSplitState> finishedSplitIds) {
        boolean requestNextSplit = true;
        for (MySqlSplitState mySqlSplitState : finishedSplitIds.values()) {
            MySqlSplit mySqlSplit = mySqlSplitState.toMySqlSplit();
            if (mySqlSplit.isBinlogSplit()) {
                LOG.info(
                        "binlog split reader suspended due to newly added table, offset {}",
                        mySqlSplitState.asBinlogSplitState().getStartingOffset());

                mySqlSourceReaderContext.resetStopBinlogSplitReader();
                suspendedBinlogSplit = toSuspendedBinlogSplit(mySqlSplit.asBinlogSplit());
                context.sendSourceEventToCoordinator(new SuspendBinlogReaderAckEvent());
                // do not request next split when the reader is suspended, the suspended reader will
                // automatically request the next split after it has been wakeup
                requestNextSplit = false;
            } else {
                finishedUnackedSplits.put(mySqlSplit.splitId(), mySqlSplit.asSnapshotSplit());
            }
        }
        // 发送切片完成事件
        reportFinishedSnapshotSplitsIfNeed();
        if (requestNextSplit) {
            context.sendSplitRequest();
        }
    }

    // 添加此reader要read的split列表,当splitEnumerator通过splitEnumeratorContext分配一个split时,将调用此方法
    // 即调用context.assignSplit(SourceSplit, int) 或者 context.assignSplits(SplitsAssignment).
    @Override
    public void addSplits(List<MySqlSplit> splits) {
        // restore for finishedUnackedSplits
        List<MySqlSplit> unfinishedSplits = new ArrayList<>();
        for (MySqlSplit split : splits) {
            LOG.info("Add Split: " + split);
            // 判断是否是snapshot还是binlog split
            if (split.isSnapshotSplit()) {
                // 如果split已经read完，放入完成集合,否则放入未完成的集合中
                MySqlSnapshotSplit snapshotSplit = split.asSnapshotSplit();
                if (snapshotSplit.isSnapshotReadFinished()) {
                    finishedUnackedSplits.put(snapshotSplit.splitId(), snapshotSplit);
                } else {
                    unfinishedSplits.add(split);
                }
            } else {
                MySqlBinlogSplit binlogSplit = split.asBinlogSplit();
                // the binlog split is suspended
                if (binlogSplit.isSuspended()) {
                    suspendedBinlogSplit = binlogSplit;
                } else if (!binlogSplit.isCompletedSplit()) {
                    // 如果binlog split未完成则加入未完成的列表中,并向splitEnumerator发送请求binlog split meta的事件
                    uncompletedBinlogSplits.put(split.splitId(), split.asBinlogSplit());
                    requestBinlogSplitMetaIfNeeded(split.asBinlogSplit());
                } else {
                    // 未完成的split集合删除该split ,未完成的集合表示没有split meta信息
                    uncompletedBinlogSplits.remove(split.splitId());
                    // 创建binlog split, 带有table schema信息
                    MySqlBinlogSplit mySqlBinlogSplit =
                            discoverTableSchemasForBinlogSplit(split.asBinlogSplit());
                    // 添加到未完成的splits,后续会进行read操作
                    unfinishedSplits.add(mySqlBinlogSplit);
                }
            }
        }
        // notify split enumerator again about the finished unacked snapshot splits
        reportFinishedSnapshotSplitsIfNeed();
        // add all un-finished splits (including binlog split) to SourceReaderBase
        if (!unfinishedSplits.isEmpty()) {
            // 当调用super.addSplits的时候,会启动fetcherManager,开始读取数据的操作
            super.addSplits(unfinishedSplits);
        }
    }

    private MySqlBinlogSplit discoverTableSchemasForBinlogSplit(MySqlBinlogSplit split) {
        final String splitId = split.splitId();
        // 如果tableSchema不存在则填充,如果已经存在,则直接返回split即可
        if (split.getTableSchemas().isEmpty()) {
            // 静态方法,构建一个mysqlConnection,可以认为就是一个jdbc连接 ,不必深入
            try (MySqlConnection jdbc = DebeziumUtils.createMySqlConnection(sourceConfig)) {
                // 根据我们sourceBuilder构建的时候给定的database和tableList来构建对应的tableId和TableChange,
                // 然后我们在面read的时候需要, 不必深入工具类
                Map<TableId, TableChanges.TableChange> tableSchemas =
                        TableDiscoveryUtils.discoverCapturedTableSchemas(sourceConfig, jdbc);
                LOG.info("The table schema discovery for binlog split {} success", splitId);
                // 构建一个带有tableSchema的MysqlBinlogSpilt,不必深入
                return MySqlBinlogSplit.fillTableSchemas(split, tableSchemas);
            } catch (SQLException e) {
                LOG.error("Failed to obtains table schemas due to {}", e.getMessage());
                throw new FlinkRuntimeException(e);
            }
        } else {
            LOG.warn(
                    "The binlog split {} has table schemas yet, skip the table schema discovery",
                    split);
            return split;
        }
    }

    // 处理source自定义事件
    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof FinishedSnapshotSplitsAckEvent) {
            FinishedSnapshotSplitsAckEvent ackEvent = (FinishedSnapshotSplitsAckEvent) sourceEvent;
            LOG.debug(
                    "The subtask {} receives ack event for {} from enumerator.",
                    subtaskId,
                    ackEvent.getFinishedSplits());
            for (String splitId : ackEvent.getFinishedSplits()) {
                this.finishedUnackedSplits.remove(splitId);
            }
        } else if (sourceEvent instanceof FinishedSnapshotSplitsRequestEvent) {
            // report finished snapshot splits
            LOG.debug(
                    "The subtask {} receives request to report finished snapshot splits.",
                    subtaskId);
            reportFinishedSnapshotSplitsIfNeed();
        } else if (sourceEvent instanceof BinlogSplitMetaEvent) {
            LOG.debug(
                    "The subtask {} receives binlog meta with group id {}.",
                    subtaskId,
                    ((BinlogSplitMetaEvent) sourceEvent).getMetaGroupId());
            fillMetaDataForBinlogSplit((BinlogSplitMetaEvent) sourceEvent);
        } else if (sourceEvent instanceof SuspendBinlogReaderEvent) {
            mySqlSourceReaderContext.setStopBinlogSplitReader();
        } else if (sourceEvent instanceof WakeupReaderEvent) {
            WakeupReaderEvent wakeupReaderEvent = (WakeupReaderEvent) sourceEvent;
            if (wakeupReaderEvent.getTarget() == SNAPSHOT_READER) {
                // 上一个spilt处理完成后继续发送切片请求
                context.sendSplitRequest();
            } else {
                if (suspendedBinlogSplit != null) {
                    context.sendSourceEventToCoordinator(
                            new LatestFinishedSplitsSizeRequestEvent());
                }
            }
        } else if (sourceEvent instanceof LatestFinishedSplitsSizeEvent) {
            if (suspendedBinlogSplit != null) {
                final int finishedSplitsSize =
                        ((LatestFinishedSplitsSizeEvent) sourceEvent).getLatestFinishedSplitsSize();
                final MySqlBinlogSplit binlogSplit =
                        toNormalBinlogSplit(suspendedBinlogSplit, finishedSplitsSize);
                suspendedBinlogSplit = null;
                this.addSplits(Collections.singletonList(binlogSplit));
            }
        } else {
            super.handleSourceEvents(sourceEvent);
        }
    }

    private void reportFinishedSnapshotSplitsIfNeed() {
        if (!finishedUnackedSplits.isEmpty()) {
            final Map<String, BinlogOffset> finishedOffsets = new HashMap<>();
            for (MySqlSnapshotSplit split : finishedUnackedSplits.values()) {
                // 发送切片ID，及最大偏移量
                finishedOffsets.put(split.splitId(), split.getHighWatermark());
            }
            FinishedSnapshotSplitsReportEvent reportEvent =
                    new FinishedSnapshotSplitsReportEvent(finishedOffsets);
            context.sendSourceEventToCoordinator(reportEvent);
            LOG.debug(
                    "The subtask {} reports offsets of finished snapshot splits {}.",
                    subtaskId,
                    finishedOffsets);
        }
    }

    // 发送请求binlogSplit meta的事件
    private void requestBinlogSplitMetaIfNeeded(MySqlBinlogSplit binlogSplit) {
        final String splitId = binlogSplit.splitId();
        if (!binlogSplit.isCompletedSplit()) {
            final int nextMetaGroupId =
                    getNextMetaGroupId(
                            binlogSplit.getFinishedSnapshotSplitInfos().size(),
                            sourceConfig.getSplitMetaGroupSize());
            BinlogSplitMetaRequestEvent splitMetaRequestEvent =
                    new BinlogSplitMetaRequestEvent(splitId, nextMetaGroupId);
            context.sendSourceEventToCoordinator(splitMetaRequestEvent);
        } else {
            LOG.info("The meta of binlog split {} has been collected success", splitId);
            this.addSplits(Collections.singletonList(binlogSplit));
        }
    }

    // 我们发送了请求meta的event后,会收到binlog split meta,我们需要填充至binlogSplit中
    private void fillMetaDataForBinlogSplit(BinlogSplitMetaEvent metadataEvent) {
        MySqlBinlogSplit binlogSplit = uncompletedBinlogSplits.get(metadataEvent.getSplitId());
        if (binlogSplit != null) {
            final int receivedMetaGroupId = metadataEvent.getMetaGroupId();
            final int expectedMetaGroupId =
                    getNextMetaGroupId(
                            binlogSplit.getFinishedSnapshotSplitInfos().size(),
                            sourceConfig.getSplitMetaGroupSize());
            if (receivedMetaGroupId == expectedMetaGroupId) {
                List<FinishedSnapshotSplitInfo> metaDataGroup =
                        metadataEvent.getMetaGroup().stream()
                                .map(FinishedSnapshotSplitInfo::deserialize)
                                .collect(Collectors.toList());
                uncompletedBinlogSplits.put(
                        binlogSplit.splitId(),
                        MySqlBinlogSplit.appendFinishedSplitInfos(binlogSplit, metaDataGroup));

                LOG.info("Fill meta data of group {} to binlog split", metaDataGroup.size());
            } else {
                LOG.warn(
                        "Received out of oder binlog meta event for split {}, the received meta group id is {}, but expected is {}, ignore it",
                        metadataEvent.getSplitId(),
                        receivedMetaGroupId,
                        expectedMetaGroupId);
            }
            requestBinlogSplitMetaIfNeeded(uncompletedBinlogSplits.get(binlogSplit.splitId()));
        } else {
            LOG.warn(
                    "Received binlog meta event for split {}, but the uncompleted split map does not contain it",
                    metadataEvent.getSplitId());
        }
    }

    private void logCurrentBinlogOffsets(List<MySqlSplit> splits, long checkpointId) {
        if (!LOG.isInfoEnabled()) {
            return;
        }
        for (MySqlSplit split : splits) {
            if (!split.isBinlogSplit()) {
                return;
            }
            BinlogOffset offset = split.asBinlogSplit().getStartingOffset();
            LOG.info("Binlog offset on checkpoint {}: {}", checkpointId, offset);
        }
    }


    // state变成不可变的state
    @Override
    protected MySqlSplit toSplitType(String splitId, MySqlSplitState splitState) {
        return splitState.toMySqlSplit();
    }
}
