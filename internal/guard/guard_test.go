package guard

import (
	"encoding/json"
	"strings"
	"testing"
)

func pl(t *testing.T, s string) []json.RawMessage {
	t.Helper()
	var p []json.RawMessage
	if err := json.Unmarshal([]byte(s), &p); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestValidar(t *testing.T) {
	g := New([]string{"test_inputs"})
	casos := []struct {
		nombre, pipeline, error string
		op                      Opciones
	}{
		{"conteo valido", `[{"$match":{"flow":"GrupoCliente-flow"}},{"$group":{"_id":"$pais","n":{"$sum":1}}}]`, "", Opciones{}},
		{"lookup permitido", `[{"$lookup":{"from":"test_inputs","localField":"correlation_id","foreignField":"correlation_id","as":"o"}}]`, "", Opciones{}},
		{"where", `[{"$match":{"$where":"true"}}]`, "no permitido", Opciones{}},
		{"function anidada", `[{"$project":{"x":{"$function":{"body":"f","args":[],"lang":"js"}}}}]`, "no permitido", Opciones{}},
		{"accumulator", `[{"$group":{"_id":null,"x":{"$accumulator":{}}}}]`, "no permitido", Opciones{}},
		{"out", `[{"$out":"x"}]`, "no permitida", Opciones{}},
		{"lookup a otra coleccion", `[{"$lookup":{"from":"users","localField":"a","foreignField":"b","as":"u"}}]`, "no es una coleccion permitida", Opciones{}},
		{"facet anidado en facet", `[{"$facet":{"a":[{"$match":{"$where":"1"}}]}}]`, "no permitido", Opciones{}},
		{"facet en analizar", `[{"$facet":{"a":[]}}]`, "$facet no se puede", Opciones{SinFacet: true}},
		{"clave repetida", `[{"$lookup":{"from":"users","from":"test_inputs","as":"x"}}]`, "repetida", Opciones{}},
	}
	for _, c := range casos {
		err := g.Validar(pl(t, c.pipeline), c.op)
		switch {
		case c.error == "" && err != nil:
			t.Errorf("%s: se esperaba valido, error: %v", c.nombre, err)
		case c.error != "" && (err == nil || !strings.Contains(err.Error(), c.error)):
			t.Errorf("%s: se esperaba error con %q, quedo %v", c.nombre, c.error, err)
		}
	}
}
