# testing-mcp

Servidor MCP de solo lectura para estudiar el contenido de los mensajes de los flows de Mule:
- **Entrada:** el XML guardado en `message_body` de `test_inputs`.
- **Salidas:** lo que Mule envió por cada mensaje a cada destino, en `message_body` de `test_outputs`. Puede ser XML o JSON.

Entrada y salidas se unen por `correlation_id`. Permite preguntar en lenguaje natural, por ejemplo:
- "¿qué porcentaje de mensajes de GrupoCliente tiene `Codigo` no numérico y `CodigoNormalizado` con valor?"
- "¿qué mensajes de Cliente no tuvieron salida a POPS?"
- "¿el `Codigo` que llegó es el mismo que se envió?"

Parte del MCP de Hermes AI (`mcp-server`) y usa el mismo guard de pipelines.

## Flows cubiertos

66 flows, todos los flows de negocio de los repos de Mule (61 con salidas):

| Área | Flows |
|---|---|
| Comercial (21) | `GrupoCliente`, `Cliente`, `Objetivo` (centros), `PuntoServicio`, `CalendarioClienteObjetivo`, `CancelacionOT`, `CatalogoServicios`, `Empresa`, `EstructuraProsegur`, `MaestroCondicionPago`, `MaestroDivisiones`, `MaestroEscalas`, `MaestroSector`, `MaestroServicios`, `MaestroSubescalas`, `ObjetivoSubtercero`, `OrdenTrabajo`, `PeriodoCierreClientes`, `RelacionCategoriaSalarialEscalaServicio`, `RespuestaOT`, `VariacionOTServicioSinOT` |
| RRHH (27) | `AbsentismoDeEmpleados`, `AsientosDeSueldos`, `CalendarioManoObra`, `CargarHorasCompensar`, `CategoriasSalariales`, `ConsultaAbsentismosEmpleados`, `ConsultaFicherosMO`, `CosteEmpleados`, `DatosLiquidacion`, `Empleados`, `EnvioFiniquitos`, `EstructuraGeografica`, `FechasFiniquitos`, `FicheroFiniquitos`, `FormacionEmpleado`, `HabilitacionesObjetivos`, `InformacionCierresFiniquitos`, `MaestroAbsentismos`, `MaestroBeneficios`, `MaestroCategorias`, `MaestroConceptos`, `MaestroLugaresPago`, `MaestroMotivosFiniquito`, `MaestroTiposContrato`, `ProcesarContratoEmpleado`, `ProcesarEmpleado`, `RetrocesoFiniquitos` |
| Selfor (4) | `HabilitacionesEmpleados`, `MaestroAccionesFormativas`, `MaestroNivelesFormacion`, `MaestroTiposHabilitaciones` |
| ERP (5) | `CostesMateriales`, `HorasReclasificacionGastos`, `MaestroTiposMateriales`, `MaterialesAFecha`, `MovimientosMateriales` |
| Facturación (5) | `CalendarioPrestacionServicios`, `ControlHorasMensuales`, `HorasTrabajadas`, `InformacionParaInformeComparativo`, `MarcaFicheroHoras` |
| Motor (2) | `ConsultaMotor`, `IntegracionWFMMotor` |
| Opera (1) | `ConsultaCuadranteParametrizado` |
| VCM (1) | `ServicioVCM` |

En `test_inputs`, cada uno se llama con el sufijo `-flow` (por ejemplo, `GrupoCliente-flow`).

El formato del mensaje de cada flow está en `conocimiento/flows.json`. Sale del XSD con que el flow de Mule valida la entrada (`validate-schema`), y cada flow indica en `xsd` el repo y el fichero. Los valores posibles de un campo no están en el XSD como lista cerrada: cuando el XSD los documenta, van en las notas del campo, tal como los escribe. Para sumar un flow, se agrega ahí: no hace falta tocar el código. Ese fichero no está en el repositorio: se copia aparte.

Los campos que van dentro de una lista (por ejemplo, `CodEmpresa` dentro de `Empresa`) no se leen uno por uno: de la lista se cuenta cuántas veces viene.

### Salidas

El formato de lo que cada flow envía a cada destino está en `conocimiento/salidas.json`. Sale de Mule:
- **El mensaje:** el DataWeave del sub-flow de backend que lo arma y lo loguea (VIG0005D), que es lo que queda en `test_outputs`.
- **Los destinos:** los `*-pro-app-config.yaml` de cada repo.

Ese fichero tampoco está en el repositorio: se copia aparte.

**Qué indica, por salida:**
- `formato`: `xml` o `json`.
- `destinos`: con las configuraciones en que aparece cada uno.
- `contenedores`: los que envuelven al mensaje, por ejemplo `Envelope/Body`.

**Qué indica, por campo:**
- `forma`: valor, lista u objeto.
- `origen`: el campo de la entrada que usa.
- `expresion`: cuando el valor no se copia tal cual.
- `condicion`: cuando el campo no siempre se envía.
- `noEstaEnLaEntrada`: el origen no está en el XSD de entrada.

Seis flows no tienen salidas porque Mule no loguea lo que envían: CostesMateriales, MaestroTiposMateriales, MaterialesAFecha, MovimientosMateriales, ConsultaMotor e IntegracionWFMMotor. La salida a `vigia (WFM)` sale de `motorwfm-main-flow`, una entrada propia por cola que no está en `flows.json`.

Una salida con `destino` vacío es un mensaje que el log cortó antes de decir el destino: está truncado y no se lee.

## Herramientas

| Herramienta | Qué hace |
|---|---|
| `diccionario` | Formato del mensaje de entrada de un flow (elemento raíz y campos con tipo, obligatorio, forma, valores posibles y notas) y de sus salidas por destino. No consulta la base. |
| `analizar_mensajes` | Lee en el servidor los campos pedidos del XML de los mensajes de un flow y ejecuta encima las etapas que arma el modelo. Con `con_salidas`, cada mensaje trae además sus salidas, sin el cuerpo. |
| `seguir_mensaje` | Un mensaje por su `correlation_id`: la entrada y todas sus salidas, por hora, y qué destinos del flow tuvieron salida. |
| `analizar_salidas` | Une cada mensaje de entrada de un flow con su salida a un destino y lee en el servidor los campos pedidos de la salida y, si se piden, de la entrada, para compararlos. |

**Qué devuelve `analizar_mensajes`:**
- **`mensajes`:** cuántos mensajes se analizaron, en total y separados en reales y mock.
- **`lectura`:** por campo, cuántos mensajes lo traen con valor, vacío, ausente o ilegible.
- **`filas`:** el resultado de las etapas que armó el modelo.

**Qué recibe cada mensaje en esas etapas:**
- `correlation_id`
- `mock` (`true`/`false`)
- `event_timestamp`: cuándo Mule recibió el mensaje (UTC)
- `message_body`: el XML completo, para poder mostrarlo
- `presencia.<campo>`
- `campos.<campo>`: el texto tal cual o, en las listas, cuántas veces viene

**Qué agrega `analizar_salidas` a cada mensaje:**
- `salida`: `{destino, event_timestamp, message_body}`, o `null` si no hubo salida a ese destino.
- `salida_truncada`: `true` si el mensaje tiene una salida truncada.
- `presencia_salida.<ruta>` y `campos_salida.<ruta>`: como en la entrada, `null` si no hubo salida.

**Cómo se pide un campo de salida:** por su ruta con puntos desde el elemento raíz, por ejemplo `Codigo` o `InputParameter.Secuencial`. Los tramos intermedios tienen que ser objetos; una lista se cuenta, igual que en la entrada.

**Qué devuelve, además de las filas:**
- **`mensajes`:** total, con salida, sin salida y sin salida pero con una truncada.
- **`lectura`:** por campo de entrada pedido.
- **`lectura_salida`:** por campo de salida pedido, sobre los mensajes con salida.

**Mock:** el parámetro `origen` elige `todos` (por defecto), `reales` o `mock`.

**Fechas:** `desde` y `hasta` limitan el periodo antes de leer el XML, así `mensajes` y `lectura` cuentan solo ese periodo. Aceptan una fecha (`2026-10-01`, interpretada en `TESTING_MCP_ZONA_HORARIA`, por defecto Europe/Madrid; `hasta` incluye el día completo) o una fecha y hora con zona (`2026-10-01T08:00:00+02:00`). Para agrupar por día, mes u hora se usa `event_timestamp` en el pipeline (`$dateToString` o `$dateTrunc`).

## Reglas

- **Sin JavaScript y solo lectura.** El guard rechaza `$where`, `$function` y `$accumulator`, además de toda etapa que escriba (`$out`, `$merge`). Solo consulta `test_inputs` y `test_outputs`. Las salidas se buscan por `correlation_id`, con el índice `correlation_id` + `destino` que crea App1.
- **Lectura del XML con texto fijo** (`$indexOfCP`, `$substrCP`, `$split`), no con regex: una regex sobre `message_body` devolvió ceros sin dar error. La lectura:
  - acepta el campo con o sin prefijo de namespace, en cualquier orden;
  - lee el contenido de un CDATA;
  - trata `<Campo/>` como campo vacío;
  - no confunde un campo de primer nivel con uno del mismo nombre dentro de una lista (por ejemplo, `Nombre` dentro de `ListadoDireccion`);
  - no confunde una lista con otro elemento cuyo nombre termina igual (por ejemplo, `Division` con `CodDivision` o `TipoDivision`).

  - lee también los elementos con atributos, por ejemplo los mensajes que repiten `xmlns` en cada elemento (`<Codigo xmlns="...">0100200</Codigo>`).

  Lo que no puede leer con seguridad (una etiqueta o un CDATA sin cerrar, o una estructura que no cuadra) lo cuenta como ilegible y lo avisa.
- **Lectura del JSON de las salidas, también con texto fijo:**
  - busca la clave como `"Campo":`;
  - delimita un objeto o una lista contando llaves o corchetes;
  - en cada nivel quita antes las listas y objetos hermanos que tienen un campo con el mismo nombre, por ejemplo `Nombre` y `DireccionList.Nombre`;
  - un texto va tal cual, sin desescapar, y un número o booleano como texto; `null` cuenta como vacío;
  - si el cuerpo se guardó escapado (`\"Campo\":`), primero lo desescapa;
  - un JSON cortado se cuenta como ilegible.

  Una llave o un corchete dentro de un texto podría confundir la delimitación.
- **Límites.** Devuelve como máximo `TESTING_MCP_LIMITE_PANTALLA` filas, con aviso si hay más. Cada consulta tiene un tiempo máximo, `TESTING_MCP_MONGO_TIMEOUT`, que Mongo también recibe como `maxTimeMS`.

## Instalación

1. Compilar (cerrar antes Claude Desktop, que deja el ejecutable bloqueado):

   ```bash
   go build -o testing-mcp.exe ./cmd/testing-mcp
   ```

2. Completar `TESTING_MCP_MONGO_URI` en el `.env` que está al lado del ejecutable (ver `.env.example`).
3. En Claude Desktop, **Settings → Developer → Edit Config**, agregar:

   ```json
   {
     "mcpServers": {
       "testing-mcp": {
         "command": "D:\\sharepoint\\mulesoft\\testing-mcp\\testing-mcp.exe"
       }
     }
   }
   ```

4. Reiniciar Claude Desktop por completo, cerrando el proceso y no solo la ventana. Hay que hacerlo también después de recompilar o de cambiar el `.env`, `flows.json` o `salidas.json`.

Para llevarlo a otra máquina hacen falta cuatro cosas en una misma carpeta: `testing-mcp.exe`, `.env`, `conocimiento/flows.json` y `conocimiento/salidas.json`. La ruta de `salidas.json` se puede cambiar con `TESTING_MCP_SALIDAS`.

## Pruebas

```bash
go test ./...
```

Necesitan `conocimiento/flows.json` y `conocimiento/salidas.json`.

Las de `internal/tools` corren contra un Mongo real si está definida `TESTING_MCP_PRUEBAS_URI`; si no, se saltan. Crean una base temporal `testing_mcp_pruebas` y la borran al terminar.
