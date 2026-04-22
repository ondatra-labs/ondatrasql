// OndatraSQL - A data pipeline runtime for DuckDB and DuckLake
// Copyright (C) 2026 Marcus Hernandez
// Licensed under the GNU AGPL v3 - see LICENSE file

// Package copyfunc registers custom DuckDB COPY TO functions via the C API.
// The Go driver does not wrap the copy function API, so we use CGo directly
// against duckdb.h which is bundled with duckdb-go-bindings.
package copyfunc

import (
	"database/sql"
	"fmt"
	"reflect"
	"runtime/cgo"
	"sync"
	"unsafe"

	bindings "github.com/duckdb/duckdb-go-bindings"
)

/*
#cgo CPPFLAGS: -I${SRCDIR}/include
#include <duckdb.h>
#include <stdlib.h>

// Forward declarations for Go callbacks
extern void goCopyBind(duckdb_copy_function_bind_info info);
extern void goCopySink(duckdb_copy_function_sink_info info, duckdb_data_chunk chunk);
extern void goCopyFinalize(duckdb_copy_function_finalize_info info);
extern void goCopyGlobalInit(duckdb_copy_function_global_init_info info);
*/
import "C"

// SinkFunc is the Go callback that receives rows during COPY TO execution.
type SinkFunc func(rows []map[string]any, options map[string]string) error

// sinkEntry holds a sink function and its column name mapping.
type sinkEntry struct {
	fn      SinkFunc
	columns []string // column names from the source table
}

// sinkRegistry maps copy function names to their callbacks and metadata.
var (
	sinkMu       sync.RWMutex
	sinkRegistry = make(map[string]*sinkEntry)
)

// RegisterSink registers a COPY TO function that routes data to a Go callback.
// After registration, `COPY table TO 'name' (FORMAT name, opt1 'val1')` will
// invoke the provided SinkFunc with the rows and options.
// RegisterSink registers a COPY TO function. columnNames are the expected
// column names from the source table — used to map positional data chunk
// columns to named keys in the rows passed to the sink function.
func RegisterSink(sqlConn *sql.Conn, name string, columnNames []string, sink SinkFunc) error {
	sinkMu.Lock()
	sinkRegistry[name] = &sinkEntry{fn: sink, columns: columnNames}
	sinkMu.Unlock()

	return sqlConn.Raw(func(driverConn any) error {
		conn := extractConnectionPtr(driverConn)
		if conn == nil {
			return fmt.Errorf("copyfunc: cannot extract duckdb_connection")
		}
		cConn := C.duckdb_connection(conn)

		cf := C.duckdb_create_copy_function()

		cName := C.CString(name)
		defer C.free(unsafe.Pointer(cName))
		C.duckdb_copy_function_set_name(cf, cName)

		// Store the function name as extra_info so callbacks can look up the SinkFunc
		nameHandle := cgo.NewHandle(name)
		C.duckdb_copy_function_set_extra_info(cf, unsafe.Pointer(&nameHandle), nil)

		C.duckdb_copy_function_set_bind(cf, C.duckdb_copy_function_bind_t(C.goCopyBind))
		C.duckdb_copy_function_set_global_init(cf, C.duckdb_copy_function_global_init_t(C.goCopyGlobalInit))
		C.duckdb_copy_function_set_sink(cf, C.duckdb_copy_function_sink_t(C.goCopySink))
		C.duckdb_copy_function_set_finalize(cf, C.duckdb_copy_function_finalize_t(C.goCopyFinalize))

		state := C.duckdb_register_copy_function(cConn, cf)
		C.duckdb_destroy_copy_function(&cf)

		if state == C.DuckDBError {
			return fmt.Errorf("copyfunc: failed to register %q", name)
		}
		return nil
	})
}

// extractConnectionPtr pulls the raw duckdb_connection pointer from *duckdb.Conn
// via reflect (the conn field is unexported).
func extractConnectionPtr(driverConn any) unsafe.Pointer {
	v := reflect.ValueOf(driverConn)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	// First field of duckdb.Conn is `conn mapping.Connection`
	// mapping.Connection = bindings.Connection = struct{Ptr unsafe.Pointer}
	connField := v.Field(0) // conn mapping.Connection
	ptrField := connField.Field(0) // Ptr unsafe.Pointer
	return unsafe.Pointer(ptrField.Pointer())
}

// sinkState holds accumulated rows during a COPY TO execution.
type sinkState struct {
	name    string
	options map[string]string
	rows    []map[string]any
	columns []string
	mu      sync.Mutex
}

//export goCopyBind
func goCopyBind(info C.duckdb_copy_function_bind_info) {
	colCount := int(C.duckdb_copy_function_bind_get_column_count(info))

	// Extract column names from the source table schema
	columns := make([]string, colCount)
	for i := 0; i < colCount; i++ {
		logType := C.duckdb_copy_function_bind_get_column_type(info, C.idx_t(i))
		// Column names come from the COPY source — we store indices as names
		// and map them in goCopySink using the column order from the source table
		columns[i] = fmt.Sprintf("col%d", i)
		C.duckdb_destroy_logical_type(&logType)
	}

	state := &sinkState{
		columns: columns,
		options: make(map[string]string),
	}

	// Extract function name from extra_info
	extraInfo := C.duckdb_copy_function_bind_get_extra_info(info)
	if extraInfo != nil {
		h := *(*cgo.Handle)(extraInfo)
		state.name = h.Value().(string)
	}

	// COPY options (api_key, etc.) are not extracted via C API in this version.
	// Sink functions should read credentials from env variables instead.

	handle := cgo.NewHandle(state)
	C.duckdb_copy_function_bind_set_bind_data(info, unsafe.Pointer(&handle), nil)
}

//export goCopyGlobalInit
func goCopyGlobalInit(info C.duckdb_copy_function_global_init_info) {
	// Nothing to initialize globally — state is in bind_data
}

//export goCopySink
func goCopySink(info C.duckdb_copy_function_sink_info, chunk C.duckdb_data_chunk) {
	bindData := C.duckdb_copy_function_sink_get_bind_data(info)
	if bindData == nil {
		return
	}
	h := *(*cgo.Handle)(bindData)
	state := h.Value().(*sinkState)

	rowCount := int(C.duckdb_data_chunk_get_size(chunk))
	colCount := int(C.duckdb_data_chunk_get_column_count(chunk))

	state.mu.Lock()
	defer state.mu.Unlock()

	// Get column names from registry (registered at RegisterSink time)
	sinkMu.RLock()
	entry := sinkRegistry[state.name]
	sinkMu.RUnlock()

	for r := 0; r < rowCount; r++ {
		row := make(map[string]any, colCount)
		for c := 0; c < colCount; c++ {
			vec := C.duckdb_data_chunk_get_vector(chunk, C.idx_t(c))
			colName := fmt.Sprintf("col%d", c)
			if entry != nil && c < len(entry.columns) {
				colName = entry.columns[c]
			}
			row[colName] = extractVectorValue(vec, r)
		}
		state.rows = append(state.rows, row)
	}
}

//export goCopyFinalize
func goCopyFinalize(info C.duckdb_copy_function_finalize_info) {
	bindData := C.duckdb_copy_function_finalize_get_bind_data(info)
	if bindData == nil {
		return
	}
	h := *(*cgo.Handle)(bindData)
	state := h.Value().(*sinkState)

	sinkMu.RLock()
	entry, ok := sinkRegistry[state.name]
	sinkMu.RUnlock()

	if !ok || entry == nil {
		cErr := C.CString(fmt.Sprintf("no sink registered for %q", state.name))
		defer C.free(unsafe.Pointer(cErr))
		C.duckdb_copy_function_finalize_set_error(info, cErr)
		return
	}

	if err := entry.fn(state.rows, state.options); err != nil {
		cErr := C.CString(err.Error())
		defer C.free(unsafe.Pointer(cErr))
		C.duckdb_copy_function_finalize_set_error(info, cErr)
	}

	// Cleanup handle
	h.Delete()
}

// extractVectorValue reads a single value from a DuckDB vector.
// Simplified — handles VARCHAR and numeric types.
func extractVectorValue(vec C.duckdb_vector, row int) any {
	logType := C.duckdb_vector_get_column_type(vec)
	defer C.duckdb_destroy_logical_type(&logType)

	typeID := C.duckdb_get_type_id(logType)
	data := C.duckdb_vector_get_data(vec)
	validity := C.duckdb_vector_get_validity(vec)

	// Check NULL
	if validity != nil {
		idx := C.idx_t(row)
		entryIdx := idx / 64
		bitIdx := idx % 64
		validitySlice := (*[1 << 30]C.uint64_t)(unsafe.Pointer(validity))
		if validitySlice[entryIdx]&(1<<bitIdx) == 0 {
			return nil
		}
	}

	switch typeID {
	case C.DUCKDB_TYPE_VARCHAR:
		strSlice := (*[1 << 30]C.duckdb_string_t)(data)
		str := strSlice[row]
		return C.GoString(C.duckdb_string_t_data(&str))
	case C.DUCKDB_TYPE_BIGINT:
		intSlice := (*[1 << 30]C.int64_t)(data)
		return int64(intSlice[row])
	case C.DUCKDB_TYPE_INTEGER:
		intSlice := (*[1 << 30]C.int32_t)(data)
		return int64(intSlice[row])
	case C.DUCKDB_TYPE_DOUBLE:
		dblSlice := (*[1 << 30]C.double)(data)
		return float64(dblSlice[row])
	case C.DUCKDB_TYPE_BOOLEAN:
		boolSlice := (*[1 << 30]C.bool)(data)
		return bool(boolSlice[row])
	default:
		// Fallback: try to read as string
		return fmt.Sprintf("<unsupported type %d>", typeID)
	}
}

// Ensure bindings package is imported (needed for CGo include path resolution).
var _ bindings.Connection
