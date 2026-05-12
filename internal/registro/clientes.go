package registro

import (
	"net"
)

// RegistroClientes mantiene el listado de conexiones activas de forma segura
type RegistroClientes struct {
	// TODO 1: agregar un campo sync.RWMutex para proteger el mapa
	clientes map[string]net.Conn
}

// NuevoRegistro crea un registro vacío
func NuevoRegistro() *RegistroClientes {
	// TODO 2: inicializar el mapa de clientes
	return nil
}

// Agregar añade un cliente al registro
func (r *RegistroClientes) Agregar(nombre string, conexion net.Conn) {
	// TODO 3: bloquear para escritura, agregar al mapa, desbloquear
}

// Eliminar remueve un cliente del registro
func (r *RegistroClientes) Eliminar(nombre string) {
	// TODO 4: bloquear para escritura, eliminar del mapa, desbloquear
}

// ObtenerConexiones devuelve una copia de todas las conexiones activas
func (r *RegistroClientes) ObtenerConexiones() []net.Conn {
	// TODO 5: bloquear para lectura, copiar conexiones a un slice, desbloquear
	return nil
}

// Cantidad devuelve el número de clientes conectados
func (r *RegistroClientes) Cantidad() int {
	// TODO 6: bloquear para lectura, retornar len del mapa, desbloquear
	return 0
}

// Nombres devuelve un slice con los nombres de los clientes
func (r *RegistroClientes) Nombres() []string {
	// TODO 7: bloquear para lectura, copiar nombres a un slice, desbloquear
	return nil
}
