package conocimiento

import (
	"strings"
	"testing"
)

func TestCargarSalidasReal(t *testing.T) {
	s, err := CargarSalidas("../../conocimiento/salidas.json")
	if err != nil {
		t.Fatal(err)
	}
	c, err := Cargar("../../conocimiento/flows.json")
	if err != nil {
		t.Fatal(err)
	}
	// todo flow con salidas es un flow de entrada conocido, salvo
	// motorwfm-main-flow (MensajeMotor por cola, sin XSD de flows.json)
	for _, f := range s.Flows {
		if _, ok := c.Flow(f.Flow); !ok && f.Flow != "motorwfm-main-flow" {
			t.Errorf("%s tiene salidas pero no esta en flows.json", f.Flow)
		}
	}
	cl, ok := s.Flow("Cliente-flow")
	if !ok {
		t.Fatal("falta Cliente-flow")
	}
	pops, ok := cl.PorDestino("POPS")
	if !ok || pops.Formato != FormatoJSON {
		t.Errorf("Cliente a POPS: %+v", pops)
	}
	vigia, ok := cl.PorDestino("VIGIA")
	if !ok || vigia.Formato != FormatoXML {
		t.Errorf("Cliente a VIGIA: %+v", vigia)
	}
	if _, ok := cl.PorDestino("NO-EXISTE"); ok {
		t.Error("destino inexistente encontrado")
	}
}

func TestDestinoEnDosSalidasRechazado(t *testing.T) {
	s := Salidas{Flows: []FlowSalidas{{Flow: "X-flow", Salidas: []Salida{
		{Subflow: "a", Formato: FormatoXML, Destinos: []DestinoSalida{{Destino: "VIGIA"}}},
		{Subflow: "b", Formato: FormatoJSON, Destinos: []DestinoSalida{{Destino: "VIGIA"}}},
	}}}}
	if err := s.validar(); err == nil || !strings.Contains(err.Error(), "VIGIA") {
		t.Errorf("se esperaba error de destino repetido, quedo %v", err)
	}
}

func TestHijosSalida(t *testing.T) {
	c := CampoSalida{Nombre: "L", Forma: FormaLista, Campos: []CampoSalida{
		{Nombre: "A", Forma: FormaValor},
		{Nombre: "O", Forma: FormaObjeto, Campos: []CampoSalida{{Nombre: "B", Forma: FormaValor}}},
	}}
	if got := strings.Join(c.Hijos(), ","); got != "A,O,B" {
		t.Errorf("Hijos = %s", got)
	}
}
