package parser

var TABLE = map[string]any{ // want `top-level TABLE declaration`
	"name": "x",
}

var SINK = map[string]any{} // want `top-level SINK declaration`

// Lower-case is fine; the rule targets the legacy uppercase names.
var table = "ok"

// Inside a function is fine — only top-level matters.
func helper() {
	TABLE := "x"
	_ = TABLE
}
