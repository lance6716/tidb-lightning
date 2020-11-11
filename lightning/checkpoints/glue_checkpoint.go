// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package checkpoints

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb-lightning/lightning/common"
	"github.com/pingcap/tidb-lightning/lightning/config"
	"github.com/pingcap/tidb-lightning/lightning/log"
	"github.com/pingcap/tidb-lightning/lightning/mydump"
	verify "github.com/pingcap/tidb-lightning/lightning/verification"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
)

const (
	retryTimeout    = 3 * time.Second
	defaultMaxRetry = 3
)

type Session interface {
	Copy() (Session, error)
	Close() error
	Execute(context.Context, string) ([]sqlexec.RecordSet, error)
	CommitTxn(context.Context) error
	RollbackTxn(context.Context)
	PrepareStmt(sql string) (stmtID uint32, paramCount int, fields []*ast.ResultField, err error)
	ExecutePreparedStmt(ctx context.Context, stmtID uint32, param []types.Datum) (sqlexec.RecordSet, error)
	DropPreparedStmt(stmtID uint32) error
}

type GlueCheckpointsDB struct {
	se     Session
	schema string
	taskID int64
}

func NewGlueCheckpointsDB(ctx context.Context, se Session, schemaName string, taskID int64) (*GlueCheckpointsDB, error) {
	var escapedSchemaName strings.Builder
	common.WriteMySQLIdentifier(&escapedSchemaName, schemaName)
	schema := escapedSchemaName.String()
	logger := log.With(zap.String("schema", schemaName))

	sql := fmt.Sprintf(CreateDBTemplate, schema)
	err := retry("create checkpoints database", logger, func() error {
		_, err := se.Execute(ctx, sql)
		return err
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	sql = fmt.Sprintf(CreateTaskTableTemplate, schema, CheckpointTableNameTask)
	err = retry("create task checkpoints table", logger, func() error {
		_, err := se.Execute(ctx, sql)
		return err
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	sql = fmt.Sprintf(CreateTableTableTemplate, schema, CheckpointTableNameTable)
	err = retry("create table checkpoints table", logger, func() error {
		_, err := se.Execute(ctx, sql)
		return err
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	sql = fmt.Sprintf(CreateEngineTableTemplate, schema, CheckpointTableNameEngine)
	err = retry("create engine checkpoints table", logger, func() error {
		_, err := se.Execute(ctx, sql)
		return err
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	sql = fmt.Sprintf(CreateChunkTableTemplate, schema, CheckpointTableNameChunk)
	err = retry("create chunks checkpoints table", logger, func() error {
		_, err := se.Execute(ctx, sql)
		return err
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &GlueCheckpointsDB{
		se:     se,
		schema: schema,
		taskID: taskID,
	}, nil
}

func (g GlueCheckpointsDB) Initialize(ctx context.Context, cfg *config.Config, dbInfo map[string]*TidbDBInfo) error {
	logger := log.L()
	err := Transact(ctx, "insert checkpoints", g.se, logger, func(c context.Context, s Session) error {
		stmtID, _, _, err := s.PrepareStmt(fmt.Sprintf(InitTaskTemplate, g.schema, CheckpointTableNameTask))
		if err != nil {
			return errors.Trace(err)
		}
		defer s.DropPreparedStmt(stmtID)
		_, err = s.ExecutePreparedStmt(c, stmtID, []types.Datum{
			types.NewIntDatum(cfg.TaskID),
			types.NewStringDatum(cfg.Mydumper.SourceDir),
			types.NewStringDatum(cfg.TikvImporter.Backend),
			types.NewStringDatum(cfg.TikvImporter.Addr),
			types.NewStringDatum(cfg.TiDB.Host),
			types.NewIntDatum(int64(cfg.TiDB.Port)),
			types.NewStringDatum(cfg.TiDB.PdAddr),
			types.NewStringDatum(cfg.TikvImporter.SortedKVDir),
			types.NewStringDatum(common.ReleaseVersion),
		})
		if err != nil {
			return errors.Trace(err)
		}

		stmtID2, _, _, err := s.PrepareStmt(fmt.Sprintf(InitTableTemplate, g.schema, CheckpointTableNameTable))
		if err != nil {
			return errors.Trace(err)
		}
		defer s.DropPreparedStmt(stmtID2)

		for _, db := range dbInfo {
			for _, table := range db.Tables {
				tableName := common.UniqueTable(db.Name, table.Name)
				_, err = s.ExecutePreparedStmt(c, stmtID2, []types.Datum{
					types.NewIntDatum(g.taskID),
					types.NewStringDatum(tableName),
					types.NewIntDatum(0),
					types.NewIntDatum(table.ID),
				})
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (g GlueCheckpointsDB) TaskCheckpoint(ctx context.Context) (*TaskCheckpoint, error) {
	logger := log.L()
	sql := fmt.Sprintf(ReadTaskTemplate, g.schema, CheckpointTableNameTask)
	purpose := "fetch task checkpoint"

	var taskCp *TaskCheckpoint
	err := retry(purpose, logger, func() error {
		rs, err := g.se.Execute(ctx, sql)
		if err != nil {
			return errors.Trace(err)
		}
		r := rs[0]
		defer r.Close()
		req := r.NewChunk()
		err = r.Next(ctx, req)
		if err != nil {
			return err
		}
		if req.NumRows() == 0 {
			return nil
		}

		row := req.GetRow(0)
		taskCp = &TaskCheckpoint{}
		taskCp.TaskId = row.GetInt64(0)
		taskCp.SourceDir = row.GetString(1)
		taskCp.Backend = row.GetString(2)
		taskCp.ImporterAddr = row.GetString(3)
		taskCp.TiDBHost = row.GetString(4)
		taskCp.TiDBPort = int(row.GetInt64(5))
		taskCp.PdAddr = row.GetString(6)
		taskCp.SortedKVDir = row.GetString(7)
		taskCp.LightningVer = row.GetString(8)
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return taskCp, nil
}

func (g GlueCheckpointsDB) Get(ctx context.Context, tableName string) (*TableCheckpoint, error) {
	cp := &TableCheckpoint{
		Engines: map[int32]*EngineCheckpoint{},
	}
	logger := log.With(zap.String("table", tableName))
	err := Transact(ctx, "read checkpoint", g.se, logger, func(c context.Context, s Session) error {
		// 1. Populate the engines.
		sql := fmt.Sprintf(ReadEngineTemplate, g.schema, CheckpointTableNameEngine)
		rs, err := g.se.Execute(ctx, sql)
		if err != nil {
			return errors.Trace(err)
		}
		r := rs[0]
		req := r.NewChunk()
		it := chunk.NewIterator4Chunk(req)
		for {
			err = r.Next(ctx, req)
			if err != nil {
				r.Close()
				return err
			}
			if req.NumRows() == 0 {
				break
			}

			for row := it.Begin(); row != it.End(); row = it.Next() {
				engineID := int32(row.GetInt64(0))
				status := uint8(row.GetUint64(1))
				cp.Engines[engineID] = &EngineCheckpoint{
					Status: CheckpointStatus(status),
				}
			}
		}
		r.Close()

		// 2. Populate the chunks.
		sql = fmt.Sprintf(ReadChunkTemplate, g.schema, CheckpointTableNameChunk)
		rs, err = g.se.Execute(ctx, sql)
		if err != nil {
			return errors.Trace(err)
		}
		r = rs[0]
		req = r.NewChunk()
		it = chunk.NewIterator4Chunk(req)
		for {
			err = r.Next(ctx, req)
			if err != nil {
				r.Close()
				return err
			}
			if req.NumRows() == 0 {
				break
			}

			for row := it.Begin(); row != it.End(); row = it.Next() {
				value := &ChunkCheckpoint{}
				engineID := int32(row.GetInt64(0))
				value.Key.Path = row.GetString(1)
				value.Key.Offset = row.GetInt64(2)
				value.FileMeta.Type = mydump.SourceType(row.GetInt64(3))
				value.FileMeta.Compression = mydump.Compression(row.GetInt64(4))
				value.FileMeta.SortKey = row.GetString(5)
				colPerm := row.GetBytes(6)
				value.Chunk.Offset = row.GetInt64(7)
				value.Chunk.EndOffset = row.GetInt64(8)
				value.Chunk.PrevRowIDMax = row.GetInt64(9)
				value.Chunk.RowIDMax = row.GetInt64(10)
				kvcBytes := row.GetUint64(11)
				kvcKVs := row.GetUint64(12)
				kvcChecksum := row.GetUint64(13)
				value.Timestamp = row.GetInt64(14)

				value.FileMeta.Path = value.Key.Path
				value.Checksum = verify.MakeKVChecksum(kvcBytes, kvcKVs, kvcChecksum)
				if err := json.Unmarshal(colPerm, &value.ColumnPermutation); err != nil {
					r.Close()
					return errors.Trace(err)
				}
				cp.Engines[engineID].Chunks = append(cp.Engines[engineID].Chunks, value)
			}
		}
		r.Close()

		// 3. Fill in the remaining table info
		sql = fmt.Sprintf(ReadTableRemainTemplate, g.schema, CheckpointTableNameTable)
		rs, err = g.se.Execute(ctx, sql)
		if err != nil {
			return errors.Trace(err)
		}
		r = rs[0]
		defer r.Close()
		req = r.NewChunk()
		err = r.Next(ctx, req)
		if err != nil {
			return err
		}
		if req.NumRows() == 0 {
			return nil
		}

		row := req.GetRow(0)
		cp.Status = CheckpointStatus(row.GetUint64(0))
		cp.AllocBase = row.GetInt64(1)
		cp.TableID = row.GetInt64(2)
		return nil
	})

	if err != nil {
		return nil, errors.Trace(err)
	}

	return cp, nil
}

func (g GlueCheckpointsDB) Close() error {
	return nil
}

func (g GlueCheckpointsDB) InsertEngineCheckpoints(ctx context.Context, tableName string, checkpointMap map[int32]*EngineCheckpoint) error {
	logger := log.With(zap.String("table", tableName))
	err := Transact(ctx, "update engine checkpoints", g.se, logger, func(c context.Context, s Session) error {
		engineStmt, _, _, err := s.PrepareStmt(fmt.Sprintf(ReplaceEngineTemplate, g.schema, CheckpointTableNameEngine))
		if err != nil {
			return errors.Trace(err)
		}
		defer s.DropPreparedStmt(engineStmt)

		chunkStmt, _, _, err := s.PrepareStmt(fmt.Sprintf(ReplaceChunkTemplate, g.schema, CheckpointTableNameChunk))
		if err != nil {
			return errors.Trace(err)
		}
		defer s.DropPreparedStmt(chunkStmt)

		for engineID, engine := range checkpointMap {
			_, err := s.ExecutePreparedStmt(c, engineStmt, []types.Datum{
				types.NewStringDatum(tableName),
				types.NewIntDatum(int64(engineID)),
				types.NewUintDatum(uint64(engine.Status)),
			})
			if err != nil {
				return errors.Trace(err)
			}
			for _, value := range engine.Chunks {
				columnPerm, err := json.Marshal(value.ColumnPermutation)
				if err != nil {
					return errors.Trace(err)
				}
				_, err = s.ExecutePreparedStmt(c, chunkStmt, []types.Datum{
					types.NewStringDatum(tableName),
					types.NewIntDatum(int64(engineID)),
					types.NewStringDatum(value.Key.Path),
					types.NewIntDatum(value.Key.Offset),
					types.NewIntDatum(int64(value.FileMeta.Type)),
					types.NewIntDatum(int64(value.FileMeta.Compression)),
					types.NewStringDatum(value.FileMeta.SortKey),
					types.NewBytesDatum(columnPerm),
					types.NewIntDatum(value.Chunk.Offset),
					types.NewIntDatum(value.Chunk.EndOffset),
					types.NewIntDatum(value.Chunk.PrevRowIDMax),
					types.NewIntDatum(value.Chunk.RowIDMax),
					types.NewIntDatum(value.Timestamp),
				})
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (g GlueCheckpointsDB) Update(checkpointDiffs map[string]*TableCheckpointDiff) {
	logger := log.L()
	chunkQuery := fmt.Sprintf(UpdateChunkTemplate, g.schema, CheckpointTableNameChunk)
	rebaseQuery := fmt.Sprintf(UpdateTableRebaseTemplate, g.schema, CheckpointTableNameTable)
	tableStatusQuery := fmt.Sprintf(UpdateTableStatusTemplate, g.schema, CheckpointTableNameTable)
	engineStatusQuery := fmt.Sprintf(UpdateEngineTemplate, g.schema, CheckpointTableNameEngine)
	err := Transact(context.Background(), "update checkpoints", g.se, logger, func(c context.Context, s Session) error {
		chunkStmt, _, _, err := s.PrepareStmt(chunkQuery)
		if err != nil {
			return errors.Trace(err)
		}
		defer s.DropPreparedStmt(chunkStmt)
		rebaseStmt, _, _, err := s.PrepareStmt(rebaseQuery)
		if err != nil {
			return errors.Trace(err)
		}
		defer s.DropPreparedStmt(rebaseStmt)
		tableStatusStmt, _, _, err := s.PrepareStmt(tableStatusQuery)
		if err != nil {
			return errors.Trace(err)
		}
		defer s.DropPreparedStmt(tableStatusStmt)
		engineStatusStmt, _, _, err := s.PrepareStmt(engineStatusQuery)
		if err != nil {
			return errors.Trace(err)
		}
		defer s.DropPreparedStmt(engineStatusStmt)

		for tableName, cpd := range checkpointDiffs {
			if cpd.hasStatus {
				_, err := s.ExecutePreparedStmt(c, tableStatusStmt, []types.Datum{
					types.NewUintDatum(uint64(cpd.status)),
					types.NewStringDatum(tableName),
				})
				if err != nil {
					return errors.Trace(err)
				}
			}
			if cpd.hasRebase {
				_, err := s.ExecutePreparedStmt(c, rebaseStmt, []types.Datum{
					types.NewIntDatum(cpd.allocBase),
					types.NewStringDatum(tableName),
				})
				if err != nil {
					return errors.Trace(err)
				}
			}
			for engineID, engineDiff := range cpd.engines {
				if engineDiff.hasStatus {
					_, err := s.ExecutePreparedStmt(c, engineStatusStmt, []types.Datum{
						types.NewUintDatum(uint64(engineDiff.status)),
						types.NewStringDatum(tableName),
						types.NewIntDatum(int64(engineID)),
					})
					if err != nil {
						return errors.Trace(err)
					}
				}
				for key, diff := range engineDiff.chunks {
					columnPerm, err := json.Marshal(diff.columnPermutation)
					if err != nil {
						return errors.Trace(err)
					}
					_, err = s.ExecutePreparedStmt(c, chunkStmt, []types.Datum{
						types.NewIntDatum(diff.pos),
						types.NewIntDatum(diff.rowID),
						types.NewUintDatum(diff.checksum.SumSize()),
						types.NewUintDatum(diff.checksum.SumKVS()),
						types.NewUintDatum(diff.checksum.Sum()),
						types.NewBytesDatum(columnPerm),
						types.NewStringDatum(tableName),
						types.NewIntDatum(int64(engineID)),
						types.NewStringDatum(key.Path),
						types.NewIntDatum(key.Offset),
					})
					if err != nil {
						return errors.Trace(err)
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		log.L().Error("save checkpoint failed", zap.Error(err))
	}
}

func (g GlueCheckpointsDB) RemoveCheckpoint(ctx context.Context, tableName string) error {
	logger := log.With(zap.String("table", tableName))
	if tableName == "all" {
		return retry("remove all checkpoints", logger, func() error {
			_, err := g.se.Execute(ctx, "DROP SCHEMA "+g.schema)
			return err
		})
	}
	var tableNameBuilder strings.Builder
	common.EscapeMySQLSingleQuote(&tableNameBuilder, tableName)
	tableName = tableNameBuilder.String()
	deleteChunkQuery := fmt.Sprintf(DeleteChunkTemplate, g.schema, CheckpointTableNameChunk)
	deleteChunkQuery = strings.ReplaceAll(deleteChunkQuery, "?", tableName)
	deleteEngineQuery := fmt.Sprintf(DeleteEngineTemplate, g.schema, CheckpointTableNameEngine)
	deleteEngineQuery = strings.ReplaceAll(deleteEngineQuery, "?", tableName)
	deleteTableQuery := fmt.Sprintf(DeleteTableTemplate, g.schema, CheckpointTableNameTable)
	deleteTableQuery = strings.ReplaceAll(deleteTableQuery, "?", tableName)

	return errors.Trace(Transact(ctx, "remove checkpoints", g.se, logger, func(c context.Context, s Session) error {
		if _, e := s.Execute(c, deleteChunkQuery); e != nil {
			return e
		}
		if _, e := s.Execute(c, deleteEngineQuery); e != nil {
			return e
		}
		if _, e := s.Execute(c, deleteTableQuery); e != nil {
			return e
		}
		return nil
	}))
}

func (g GlueCheckpointsDB) MoveCheckpoints(ctx context.Context, taskID int64) error {
	newSchema := fmt.Sprintf("`%s.%d.bak`", g.schema[1:len(g.schema)-1], taskID)
	logger := log.With(zap.Int64("taskID", taskID))
	err := retry("create backup checkpoints schema", logger, func() error {
		_, err := g.se.Execute(ctx, "CREATE SCHEMA IF NOT EXISTS "+newSchema)
		return err
	})
	if err != nil {
		return err
	}
	for _, tbl := range []string{CheckpointTableNameChunk, CheckpointTableNameEngine,
		CheckpointTableNameTable, CheckpointTableNameTask} {
		query := fmt.Sprintf("RENAME TABLE %[1]s.%[3]s TO %[2]s.%[3]s", g.schema, newSchema, tbl)
		err := retry(fmt.Sprintf("move %s checkpoints table", tbl), logger, func() error {
			_, err := g.se.Execute(ctx, query)
			return err
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (g GlueCheckpointsDB) IgnoreErrorCheckpoint(ctx context.Context, tableName string) error {
	logger := log.With(zap.String("table", tableName))

	var colName string
	if tableName == "all" {
		// This will expand to `WHERE 'all' = 'all'` and effectively allowing
		// all tables to be included.
		colName = "'all'"
	} else {
		colName = "table_name"
	}

	var tableNameBuilder strings.Builder
	common.EscapeMySQLSingleQuote(&tableNameBuilder, tableName)
	tableName = tableNameBuilder.String()

	engineQuery := fmt.Sprintf(`
		UPDATE %s.%s SET status = %d WHERE %s = %s AND status <= %d;
	`, g.schema, CheckpointTableNameEngine, CheckpointStatusLoaded, colName, tableName, CheckpointStatusMaxInvalid)
	tableQuery := fmt.Sprintf(`
		UPDATE %s.%s SET status = %d WHERE %s = %s AND status <= %d;
	`, g.schema, CheckpointTableNameTable, CheckpointStatusLoaded, colName, tableName, CheckpointStatusMaxInvalid)
	return errors.Trace(Transact(ctx, "ignore error checkpoints", g.se, logger, func(c context.Context, s Session) error {
		if _, e := s.Execute(c, engineQuery); e != nil {
			return e
		}
		if _, e := s.Execute(c, tableQuery); e != nil {
			return e
		}
		return nil
	}))
}

func (g GlueCheckpointsDB) DestroyErrorCheckpoint(ctx context.Context, tableName string) ([]DestroyedTableCheckpoint, error) {
	logger := log.With(zap.String("table", tableName))

	var colName, aliasedColName string

	if tableName == "all" {
		// These will expand to `WHERE 'all' = 'all'` and effectively allowing
		// all tables to be included.
		colName = "'all'"
		aliasedColName = "'all'"
	} else {
		colName = "table_name"
		aliasedColName = "t.table_name"
	}

	var tableNameBuilder strings.Builder
	common.EscapeMySQLSingleQuote(&tableNameBuilder, tableName)
	tableName = tableNameBuilder.String()

	selectQuery := fmt.Sprintf(`
		SELECT
			t.table_name,
			COALESCE(MIN(e.engine_id), 0),
			COALESCE(MAX(e.engine_id), -1)
		FROM %[1]s.%[4]s t
		LEFT JOIN %[1]s.%[5]s e ON t.table_name = e.table_name
		WHERE %[2]s = %[6]s AND t.status <= %[3]d
		GROUP BY t.table_name;
	`, g.schema, aliasedColName, CheckpointStatusMaxInvalid, CheckpointTableNameTable, CheckpointTableNameEngine, tableName)
	deleteChunkQuery := fmt.Sprintf(`
		DELETE FROM %[1]s.%[4]s WHERE table_name IN (SELECT table_name FROM %[1]s.%[5]s WHERE %[2]s = %[6]s AND status <= %[3]d)
	`, g.schema, colName, CheckpointStatusMaxInvalid, CheckpointTableNameChunk, CheckpointTableNameTable, tableName)
	deleteEngineQuery := fmt.Sprintf(`
		DELETE FROM %[1]s.%[4]s WHERE table_name IN (SELECT table_name FROM %[1]s.%[5]s WHERE %[2]s = %[6]s AND status <= %[3]d)
	`, g.schema, colName, CheckpointStatusMaxInvalid, CheckpointTableNameEngine, CheckpointTableNameTable, tableName)
	deleteTableQuery := fmt.Sprintf(`
		DELETE FROM %s.%s WHERE %s = %s AND status <= %d
	`, g.schema, CheckpointTableNameTable, colName, tableName, CheckpointStatusMaxInvalid)

	var targetTables []DestroyedTableCheckpoint
	err := Transact(ctx, "destroy error checkpoints", g.se, logger, func(c context.Context, s Session) error {
		targetTables = nil
		rs, err := s.Execute(c, selectQuery)
		if err != nil {
			return errors.Trace(err)
		}
		r := rs[0]
		req := r.NewChunk()
		it := chunk.NewIterator4Chunk(req)
		for {
			err = r.Next(ctx, req)
			if err != nil {
				r.Close()
				return err
			}
			if req.NumRows() == 0 {
				break
			}

			for row := it.Begin(); row != it.End(); row = it.Next() {
				var dtc DestroyedTableCheckpoint
				dtc.TableName = row.GetString(0)
				dtc.MinEngineID = int32(row.GetInt64(1))
				dtc.MaxEngineID = int32(row.GetInt64(2))
			}
		}
		r.Close()

		if _, e := s.Execute(c, deleteChunkQuery); e != nil {
			return errors.Trace(e)
		}
		if _, e := s.Execute(c, deleteEngineQuery); e != nil {
			return errors.Trace(e)
		}
		if _, e := s.Execute(c, deleteTableQuery); e != nil {
			return errors.Trace(e)
		}
		return nil
	})

	if err != nil {
		return nil, errors.Trace(err)
	}

	return targetTables, nil
}

func (g GlueCheckpointsDB) DumpTables(ctx context.Context, csv io.Writer) error {
	return errors.Errorf("dumping glue checkpoint into CSV not unsupported")
}

func (g GlueCheckpointsDB) DumpEngines(ctx context.Context, csv io.Writer) error {
	return errors.Errorf("dumping glue checkpoint into CSV not unsupported")
}

func (g GlueCheckpointsDB) DumpChunks(ctx context.Context, csv io.Writer) error {
	return errors.Errorf("dumping glue checkpoint into CSV not unsupported")
}

func Transact(ctx context.Context, purpose string, se Session, parentLogger log.Logger, action func(context.Context, Session) error) error {
	s, err := se.Copy()
	if err != nil {
		return errors.Annotate(err, "can't get a new session to perform Transact")
	}
	//sctx, ok := se.(sessionctx.Context)
	//if !ok {
	//	return errors.New("can't recover sessionctx.Context from glue")
	//}
	//s, err := session.CreateSession(sctx.GetStore())
	//if err != nil {
	//	return errors.Annotate(err, "create new session in GlueCheckpointsDB")
	//}
	defer s.Close()

	return retry(purpose, parentLogger, func() error {
		_, err = s.Execute(ctx, "BEGIN")
		if err != nil {
			return errors.Annotate(err, "begin transaction failed")
		}
		err = action(ctx, s)
		if err != nil {
			// session.RollbackTxn doesn't return error
			s.RollbackTxn(ctx)
		}
		err = s.CommitTxn(ctx)
		if err != nil {
			return errors.Annotate(err, "commit transaction failed")
		}
		return nil
	})
}

// copy SQLWithRetry.perform
func retry(purpose string, parentLogger log.Logger, action func() error) error {
	var err error
	for i := 0; i < defaultMaxRetry; i++ {
		logger := parentLogger.With(zap.Int("retryCnt", i))

		if i > 0 {
			logger.Warn(purpose + " retry start")
			time.Sleep(retryTimeout)
		}

		err = action()

		switch {
		case err == nil:
			return nil
		case common.IsRetryableError(err):
			logger.Warn(purpose+" failed but going to try again", log.ShortError(err))
			continue
		default:
			return err
		}
	}
	return err
}
