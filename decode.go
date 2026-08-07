// decode.go — turning raw WAL bytes into structured Change values.
//
// The pgoutput plugin sends a stream of protocol messages. Most of them are
// low-level bookkeeping (begin/commit, keepalives, relation metadata...).
// This file deals with the four message kinds that represent actual data
// changes: Insert, Update, Delete and Truncate.

package phylax

import (
	"fmt"

	"github.com/jackc/pglogrepl"
)

// Change describes one logical change on a single row — or, for a TRUNCATE,
// on a whole table.
type Change struct {
	// Table is the name of the table the change happened on.
	Table string

	// Operation is one of "insert", "update", "delete" or "truncate".
	// A truncate change means every row in Table was removed: it carries no
	// row data (OldRow and NewRow are nil), and TRUNCATE a, b, c emits one
	// truncate change per table.
	Operation string

	// OldRow holds the pre-change column values (nil for inserts and
	// truncates).
	OldRow map[string]any

	// NewRow holds the post-change column values (nil for deletes and
	// truncates).
	NewRow map[string]any
}

// Decode parses one chunk of WAL data and returns the Changes it describes.
// It returns an empty slice when the message carries no row change
// (relation metadata, transaction begin/commit, keepalives, ...). Most
// messages yield at most one Change; a TRUNCATE statement can truncate
// several tables at once, so it yields one Change per truncated table.
//
// Every successfully decoded Change increments metrics.ChangesProcessed,
// if metrics is non-nil.
//
// Relation metadata is cached in `relations` and reused across calls: the
// server sends a RelationMessage the first time a table is referenced, and
// afterwards row data is compact — tuples reference columns by index and
// rely on the cached relation for the column names.
func Decode(walData []byte, relations map[uint32]*pglogrepl.RelationMessage, metrics *Metrics) ([]*Change, error) {
	msg, err := pglogrepl.Parse(walData)
	if err != nil {
		return nil, fmt.Errorf("parsing WAL data: %w", err)
	}

	switch m := msg.(type) {
	case *pglogrepl.RelationMessage:
		// Cache the table metadata so later row messages can be decoded.
		relations[m.RelationID] = m
		return nil, nil

	case *pglogrepl.InsertMessage:
		rel, err := relationFor(relations, m.RelationID)
		if err != nil {
			return nil, err
		}
		return counted(metrics, &Change{
			Table:     rel.RelationName,
			Operation: "insert",
			NewRow:    tupleToMap(m.Tuple, rel),
		}), nil

	case *pglogrepl.UpdateMessage:
		rel, err := relationFor(relations, m.RelationID)
		if err != nil {
			return nil, err
		}
		return counted(metrics, &Change{
			Table:     rel.RelationName,
			Operation: "update",
			OldRow:    tupleToMap(m.OldTuple, rel),
			NewRow:    tupleToMap(m.NewTuple, rel),
		}), nil

	case *pglogrepl.DeleteMessage:
		rel, err := relationFor(relations, m.RelationID)
		if err != nil {
			return nil, err
		}
		return counted(metrics, &Change{
			Table:     rel.RelationName,
			Operation: "delete",
			OldRow:    tupleToMap(m.OldTuple, rel),
		}), nil

	case *pglogrepl.TruncateMessage:
		// TRUNCATE empties whole tables. One statement can truncate several
		// at once (TRUNCATE a, b, c), so emit one table-level change per
		// relation id. Relation metadata arrives before any data message
		// referencing a table, so the cache must already hold every id.
		if len(m.RelationIDs) == 0 {
			return nil, nil
		}
		changes := make([]*Change, 0, len(m.RelationIDs))
		for _, relID := range m.RelationIDs {
			rel, err := relationFor(relations, relID)
			if err != nil {
				return nil, err
			}
			changes = append(changes, &Change{Table: rel.RelationName, Operation: "truncate"})
		}
		if metrics != nil {
			metrics.ChangesProcessed.Add(int64(len(changes)))
		}
		return changes, nil

	default:
		// Begin/Commit/Type/Origin messages carry no row change.
		return nil, nil
	}
}

// counted returns one decoded change in a single-element slice (the common
// case) and counts it in the processed-change metrics.
func counted(metrics *Metrics, change *Change) []*Change {
	if metrics != nil {
		metrics.ChangesProcessed.Add(1)
	}
	return []*Change{change}
}

// relationFor looks up the cached metadata for a relation ID. Every data
// message must be preceded by its RelationMessage; a missing entry means
// the stream is out of sync, so treat it as an error rather than panic.
func relationFor(relations map[uint32]*pglogrepl.RelationMessage, relationID uint32) (*pglogrepl.RelationMessage, error) {
	rel := relations[relationID]
	if rel == nil {
		return nil, fmt.Errorf("no cached relation metadata for relation ID %d", relationID)
	}
	return rel, nil
}

// tupleToMap converts a protocol tuple (an ordered list of values) into a
// map of column name -> value, using the relation metadata for names.
//
// The pgoutput plugin encodes tuple values in several ways:
//
//   - 'n': NULL — stored as Go nil,
//   - 'u': unchanged TOAST column — the value was not sent because it is
//     large and unchanged; we skip it so the previous value is kept,
//   - 't': plain text — decoded directly as a string.
func tupleToMap(tuple *pglogrepl.TupleData, rel *pglogrepl.RelationMessage) map[string]any {
	if tuple == nil {
		return nil
	}

	result := map[string]any{}
	for i, col := range tuple.Columns {
		colName := rel.Columns[i].Name

		switch col.DataType {
		case 'n':
			result[colName] = nil
		case 'u':
			continue // unchanged TOAST column — skip, do not overwrite
		case 't':
			result[colName] = string(col.Data) // text-format value
		}
	}
	return result
}
