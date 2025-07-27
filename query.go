package sql_exporter

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/burningalchemist/sql_exporter/config"
	"github.com/burningalchemist/sql_exporter/errors"
)

// Query wraps a sql.Stmt and all the metrics populated from it. It helps extract keys and values from result rows.
type Query struct {
	config         *config.QueryConfig
	metricFamilies []*MetricFamily
	// columnTypes maps column names to the column type expected by metrics: key (string) or value (float64).
	columnTypes columnTypeMap
	logContext  string

	conn *sql.DB
	stmt *sql.Stmt
}

type (
	columnType    int
	columnTypeMap map[string]columnType
)

const (
	columnTypeKey   columnType = 1
	columnTypeValue columnType = 2
	columnTypeTime  columnType = 3
)

// NewQuery returns a new Query that will populate the given metric families.
func NewQuery(logContext string, qc *config.QueryConfig, metricFamilies ...*MetricFamily) (*Query, errors.WithContext) {
	logContext = TrimMissingCtx(fmt.Sprintf(`%s,query=%s`, logContext, qc.Name))

	columnTypes := make(columnTypeMap)

	for _, mf := range metricFamilies {
		// Create a map of output columns created by transformations
		transformedColumns := make(map[string]bool)
		for _, lagCalc := range mf.config.LagCalculations {
			transformedColumns[lagCalc.OutputColumn] = true
			// Add source columns to columnTypes since they're needed from SQL
			// Use columnTypeKey since timestamp values are strings, not numbers
			if err := setColumnType(logContext, lagCalc.SourceColumn, columnTypeKey, columnTypes); err != nil {
				return nil, err
			}
		}

		// Add columns used in row filters
		for _, filter := range mf.config.RowFilters {
			if err := setColumnType(logContext, filter.Column, columnTypeKey, columnTypes); err != nil {
				return nil, err
			}
		}

		for _, kcol := range mf.config.KeyLabels {
			if err := setColumnType(logContext, kcol, columnTypeKey, columnTypes); err != nil {
				return nil, err
			}
		}
		for _, vcol := range mf.config.Values {
			// Skip columns that are created by transformations
			if !transformedColumns[vcol] {
				if err := setColumnType(logContext, vcol, columnTypeValue, columnTypes); err != nil {
					return nil, err
				}
			}
		}
		if mf.config.TimestampValue != "" {
			if err := setColumnType(logContext, mf.config.TimestampValue, columnTypeTime, columnTypes); err != nil {
				return nil, err
			}
		}
	}

	q := Query{
		config:         qc,
		metricFamilies: metricFamilies,
		columnTypes:    columnTypes,
		logContext:     logContext,
	}

	// Debug logging to see what columns we're expecting
	expectedColumns := make([]string, 0, len(columnTypes))
	for col := range columnTypes {
		expectedColumns = append(expectedColumns, col)
	}
	slog.Debug("Expected columns from SQL", "logContext", logContext, "columns", expectedColumns)

	return &q, nil
}

// setColumnType stores the provided type for a given column, checking for conflicts in the process.
func setColumnType(logContext, columnName string, ctype columnType, columnTypes columnTypeMap) errors.WithContext {
	previousType, found := columnTypes[columnName]
	if found {
		if previousType != ctype {
			return errors.Errorf(logContext, "column %q used both as key and value", columnName)
		}
	} else {
		columnTypes[columnName] = ctype
	}
	return nil
}

// Collect is the equivalent of prometheus.Collector.Collect() but takes a context to run in and a database to run on.
func (q *Query) Collect(ctx context.Context, conn *sql.DB, ch chan<- Metric) {
	collectStart := time.Now()

	if ctx.Err() != nil {
		ch <- NewInvalidMetric(errors.Wrap(q.logContext, ctx.Err()))
		return
	}

	rows, err := q.run(ctx, conn)
	if err != nil {
		ch <- NewInvalidMetric(err)
		return
	}
	defer rows.Close()

	dest, err := q.scanDest(rows)
	if err != nil {
		if config.IgnoreMissingVals {
			slog.Warn("Ignoring missing values", "logContext", q.logContext)
			return
		}
		ch <- NewInvalidMetric(err)
		return
	}

	totalRowsProcessed := 0
	totalRowsFiltered := 0
	metricsGenerated := 0

	for rows.Next() {
		totalRowsProcessed++

		row, err := q.scanRow(rows, dest)
		if err != nil {
			ch <- NewInvalidMetric(err)
			continue
		}

		// Apply row filtering and transformations for each metric family
		for _, mf := range q.metricFamilies {
			// Apply row filters - skip row if it doesn't match
			if !q.shouldIncludeRow(row, mf.config) {
				totalRowsFiltered++
				continue
			}

			// Apply lag calculations and other transformations
			transformedRow := q.applyTransformations(row, mf.config)

			mf.Collect(transformedRow, ch)
			metricsGenerated++
		}
	}

	if err1 := rows.Err(); err1 != nil {
		ch <- NewInvalidMetric(errors.Wrap(q.logContext, err1))
	}

	// Log performance summary
	slog.Debug("Query collection completed",
		"logContext", q.logContext,
		"duration_ms", time.Since(collectStart).Milliseconds(),
		"rows_processed", totalRowsProcessed,
		"rows_filtered", totalRowsFiltered,
		"metrics_generated", metricsGenerated,
	)
}

// run executes the query on the provided database, in the provided context.
func (q *Query) run(ctx context.Context, conn *sql.DB) (*sql.Rows, errors.WithContext) {
	if slog.Default().Enabled(ctx, slog.LevelDebug) {
		start := time.Now()
		defer func() {
			slog.Debug("Query execution time", "logContext", q.logContext, "duration", time.Since(start))
		}()
	}

	if q.conn != nil && q.conn != conn {
		panic(fmt.Sprintf("[%s] Expecting to always run on the same database handle", q.logContext))
	}

	if q.config.NoPreparedStatement {
		rows, err := conn.QueryContext(ctx, q.config.Query)
		return rows, errors.Wrap(q.logContext, err)
	}

	if q.stmt == nil {
		stmt, err := conn.PrepareContext(ctx, q.config.Query)
		if err != nil {
			return nil, errors.Wrapf(q.logContext, err, "prepare query failed")
		}
		q.conn = conn
		q.stmt = stmt
	}
	rows, err := q.stmt.QueryContext(ctx)
	return rows, errors.Wrap(q.logContext, err)
}

// scanDest creates a slice to scan the provided rows into, with strings for keys, float64s for values and interface{}
// for any extra columns.
func (q *Query) scanDest(rows *sql.Rows) ([]any, errors.WithContext) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrap(q.logContext, err)
	}
	slog.Debug("Returned columns", "logContext", q.logContext, "columns", columns)
	// Create the slice to scan the row into, with strings for keys and float64s for values.
	dest := make([]any, 0, len(columns))
	have := make(map[string]bool, len(q.columnTypes))
	for i, column := range columns {
		switch q.columnTypes[column] {
		case columnTypeKey:
			dest = append(dest, new(sql.NullString))
			have[column] = true
		case columnTypeValue:
			dest = append(dest, new(sql.NullFloat64))
			have[column] = true
		case columnTypeTime:
			dest = append(dest, new(sql.NullTime))
			have[column] = true
		default:
			if column == "" {
				slog.Debug("Unnamed column", "logContext", q.logContext, "column", i)
			} else {
				slog.Debug("Extra column returned by query", "logContext", q.logContext, "column", column)
			}
			dest = append(dest, new(any))
		}
	}

	// Not all requested columns could be mapped, fail.
	if len(have) != len(q.columnTypes) {
		missing := make([]string, 0, len(q.columnTypes)-len(have))
		for c := range q.columnTypes {
			if !have[c] {
				missing = append(missing, c)
			}
		}
		return nil, errors.Errorf(q.logContext, "Missing values for the requested columns: %q", missing)
	}

	return dest, nil
}

// scanRow scans the current row into a map of column name to value, with string values for key columns and float64
// values for value columns, using dest as a buffer.
func (q *Query) scanRow(rows *sql.Rows, dest []any) (map[string]any, errors.WithContext) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, errors.Wrap(q.logContext, err)
	}

	// Scan the row content into dest.
	if err := rows.Scan(dest...); err != nil {
		return nil, errors.Wrapf(q.logContext, err, "scanning of query result failed")
	}

	// Pick all values we're interested in into a map.
	result := make(map[string]any, len(q.columnTypes))
	for i, column := range columns {
		switch q.columnTypes[column] {
		case columnTypeKey:
			if !dest[i].(*sql.NullString).Valid {
				slog.Debug("Key column is NULL", "logContext", q.logContext, "column", column)
			}
			result[column] = *dest[i].(*sql.NullString)
		case columnTypeTime:
			if !dest[i].(*sql.NullTime).Valid {
				slog.Debug("Time column is NULL", "logContext", q.logContext, "column", column)
			}
			result[column] = *dest[i].(*sql.NullTime)
		case columnTypeValue:
			if !dest[i].(*sql.NullFloat64).Valid {
				slog.Debug("Value column is NULL", "logContext", q.logContext, "column", column)
			}
			result[column] = *dest[i].(*sql.NullFloat64)
		}
	}
	return result, nil
}

// shouldIncludeRow checks if a row matches the configured row filters
func (q *Query) shouldIncludeRow(row map[string]any, metric *config.MetricConfig) bool {
	for _, filter := range metric.RowFilters {
		if !q.applyRowFilter(row, filter) {
			return false
		}
	}
	return true
}

// applyRowFilter applies a single row filter to determine if row should be included
func (q *Query) applyRowFilter(row map[string]any, filter config.RowFilter) bool {
	value, exists := row[filter.Column]
	if !exists {
		return false
	}

	// Handle sql.NullString, sql.NullFloat64, sql.NullTime types from updated codebase
	var valueStr string
	switch v := value.(type) {
	case sql.NullString:
		if !v.Valid {
			return false
		}
		valueStr = v.String
	case sql.NullFloat64:
		if !v.Valid {
			return false
		}
		valueStr = fmt.Sprintf("%v", v.Float64)
	case sql.NullTime:
		if !v.Valid {
			return false
		}
		valueStr = v.Time.Format("2006-01-02 15:04:05.000 UTC")
	default:
		valueStr = fmt.Sprintf("%v", value)
	}

	switch filter.Operator {
	case "equals":
		return valueStr == filter.Value
	case "not_equals":
		return valueStr != filter.Value
	case "in":
		for _, v := range filter.Values {
			if valueStr == v {
				return true
			}
		}
		return false
	case "not_in":
		for _, v := range filter.Values {
			if valueStr == v {
				return false
			}
		}
		return true
	case "contains":
		return strings.Contains(valueStr, filter.Value)
	default:
		slog.Warn("Unknown filter operator", "operator", filter.Operator)
		return true
	}
}

// applyTransformations applies configured transformations like lag calculations to a row
func (q *Query) applyTransformations(row map[string]any, metric *config.MetricConfig) map[string]any {
	result := make(map[string]any)

	// Copy original row data
	for k, v := range row {
		result[k] = v
	}

	// Apply lag calculations
	for _, lagCalc := range metric.LagCalculations {
		if sourceValue, exists := row[lagCalc.SourceColumn]; exists {
			lagSeconds := q.calculateLag(sourceValue, lagCalc.TimestampFormat)
			// Create a sql.NullFloat64 to match the expected type system
			result[lagCalc.OutputColumn] = sql.NullFloat64{Float64: lagSeconds, Valid: lagSeconds != 0}
		}
	}

	// Apply column filtering if specified
	if len(metric.ColumnFilters) > 0 {
		filtered := make(map[string]any)
		for _, column := range metric.ColumnFilters {
			if value, exists := result[column]; exists {
				filtered[column] = value
			}
		}
		return filtered
	}

	return result
}

// calculateLag calculates the lag in seconds between a timestamp and current time
func (q *Query) calculateLag(timestampValue any, format string) float64 {
	if timestampValue == nil {
		return 0
	}

	var timestampStr string

	// Handle different timestamp value types from the updated codebase
	switch v := timestampValue.(type) {
	case sql.NullString:
		if !v.Valid {
			return 0
		}
		timestampStr = v.String
	case sql.NullTime:
		if !v.Valid {
			return 0
		}
		// Calculate lag directly from time.Time
		return time.Since(v.Time).Seconds()
	case string:
		timestampStr = v
	default:
		timestampStr = fmt.Sprintf("%v", timestampValue)
	}

	if timestampStr == "" {
		return 0
	}

	// Default format for Trino timestamps
	if format == "" {
		format = "2006-01-02 15:04:05.000 UTC"
	}

	// Parse the timestamp
	parsedTime, err := time.Parse(format, timestampStr)
	if err != nil {
		slog.Warn("Failed to parse timestamp for lag calculation", "timestamp", timestampStr, "format", format, "error", err)
		return 0
	}

	// Calculate lag in seconds
	lag := time.Since(parsedTime).Seconds()
	return lag
}
