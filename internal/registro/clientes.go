package registro

import (
	"net"
	"sync"
)

// RegistroClientes mantiene el listado de conexiones activas de forma segura
type RegistroClientes struct {
	// TODO 1: agregar un campo sync.RWMutex para proteger el mapa
	mutex    sync.RWMutex
	clientes map[string]net.Conn
}

// NuevoRegistro crea un registro vacío
func NuevoRegistro() *RegistroClientes {
	// TODO 2: inicializar el mapa de clientes
	return &RegistroClientes{

		clientes: make(map[string]net.Conn),
	}

}

// Agregar añade un cliente al registro
func (r *RegistroClientes) Agregar(nombre string, conexion net.Conn) {
	// TODO 3: bloquear para escritura, agregar al mapa, desbloquear
	r.mutex.Lock()

	defer r.mutex.Unlock()
	r.clientes[nombre] = conexion
}

// Eliminar remueve un cliente del registro
func (r *RegistroClientes) Eliminar(nombre string) {
	// TODO 4: bloquear para escritura, eliminar del mapa, desbloquear
	r.mutex.Lock()

	defer r.mutex.Unlock()
	delete(r.clientes, nombre)
}

// ObtenerConexiones devuelve una copia de todas las conexiones activas
func (r *RegistroClientes) ObtenerConexiones() []net.Conn {
	// TODO 5: bloquear para lectura, copiar conexiones a un slice, desbloquear
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	conexiones := make([]net.Conn, 0, len(r.clientes))
	for _, conn := range r.clientes {
		conexiones = append(conexiones, conn)
	}

	return conexiones
}

// Cantidad devuelve el número de clientes conectados
func (r *RegistroClientes) Cantidad() int {
	// TODO 6: bloquear para lectura, retornar len del mapa, desbloquear
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	return len(r.clientes)
}

// Nombres devuelve un slice con los nombres de los clientes
func (r *RegistroClientes) Nombres() []string {
	// TODO 7: bloquear para lectura, copiar nombres a un slice, desbloquear
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	nombres := make([]string, 0, len(r.clientes))

	for nombre := range r.clientes {
		nombres = append(nombres, nombre)
	}

	return nombres
}

// ObtenerClientes devuelve una copia del mapa de clientes
func (r *RegistroClientes) ObtenerClientes() map[string]net.Conn {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	copia := make(map[string]net.Conn)
	for nombre, conexion := range r.clientes {
		copia[nombre] = conexion
	}
	return copia
}
