package conocimiento

import (
	"strings"
	"testing"
)

func TestCargarFicheroReal(t *testing.T) {
	c, err := Cargar("../../conocimiento/flows.json")
	if err != nil {
		t.Fatal(err)
	}
	nombres := c.NombresFlows()
	want := []string{"GrupoCliente-flow", "Cliente-flow", "Objetivo-flow", "PuntoServicio-flow"}
	if len(nombres) < len(want) || strings.Join(nombres[:len(want)], ",") != strings.Join(want, ",") {
		t.Errorf("flows = %s", strings.Join(nombres, ","))
	}
	for _, f := range c.Flows {
		if len(f.Campos) == 0 {
			t.Errorf("%s no tiene campos", f.Flow)
		}
	}
}

func TestCampoRepetidoRechazado(t *testing.T) {
	f := Flow{Flow: "X-flow", ElementoRaiz: "XRequest", Campos: []Campo{
		{Nombre: "Codigo", Forma: FormaValor},
		{Nombre: "Codigo", Forma: FormaValor},
	}}
	if err := f.validar(); err == nil || !strings.Contains(err.Error(), "repetido") {
		t.Errorf("se esperaba error de campo repetido, quedo %v", err)
	}
}
