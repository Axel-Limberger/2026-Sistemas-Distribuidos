package dht

import (
	"net/rpc"
	"sync"
)

// NodoChord simula un nodo en un anillo Chord de 8 bits (0-255).
// NOTA: Esta es una version simplificada de Chord para fines educativos.
// Cada vecino (sucesor/predecesor/entrada de finger table) se guarda como
// Direccion (para RPC) y como ID lógico (para calculo de rangos), porque
// el ID no se puede derivar del puerto (varios nodos comparten puerto 5000
// en setups con Docker).
type NodoChord struct {
	ID             int
	Direccion      string
	Sucesor        string
	SucesorID      int
	Predecesor     string
	PredecesorID   int
	FingerTable    [3]string
	FingerTableIDs [3]int
	Datos          map[int]string
	mu             sync.RWMutex
}

// TODO 8: Implementar NuevoNodo.
// Inicializar el nodo con ID, Direccion, sucesor (e ID lógico del sucesor),
// predecesor (e ID lógico del predecesor) dados.
// Construir la FingerTable con 3 entradas.
func NuevoNodo(id int, Direccion, sucesor string, sucesorID int, predecesor string, predecesorID int) *NodoChord {
	// COMPLETAR
	nodo := &NodoChord{
		ID:           id,
		Direccion:    Direccion,
		Sucesor:      sucesor,
		SucesorID:    sucesorID,
		Predecesor:   predecesor,
		PredecesorID: predecesorID,
		Datos:        make(map[int]string),
	}
	for i := 0; i < 3; i++ {
		nodo.FingerTable[i] = sucesor
		nodo.FingerTableIDs[i] = sucesorID
	}
	return nodo
}

// ActualizarAnillo reconfigura el sucesor y predecesor del nodo.
// Lo invoca bucleEstabilizacionChord del main cada 10 segundos
// después de recorrer la membresía descubierta por Gossip.
// Sin esto, el anillo Chord quedaría estático en la configuración inicial
// (PEERS) y nunca integraría los nodos descubiertos dinámicamente via Gossip.
// NOTA sobre crash de un nodo: si un miembro deja de responder, Identificarse
// falla y ese nodo se excluye de la reconfiguración, auto-reparando el anillo en ~10s.
// La membresía de Gossip sigue listando al nodo caído (no hay detector de
// fallos en este PG3 simplificado), y los datos del rango del nodo caído
// se pierden (PG3 no replica).
func (n *NodoChord) ActualizarAnillo(sucesor string, sucesorID int, predecesor string, predecesorID int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Sucesor = sucesor
	n.SucesorID = sucesorID
	n.Predecesor = predecesor
	n.PredecesorID = predecesorID
	for i := 0; i < 3; i++ {
		n.FingerTable[i] = sucesor
		n.FingerTableIDs[i] = sucesorID
	}
}

// TODO 9: Implementar EsResponsable.
// Retorna true si la clave esta en el intervalo (n.PredecesorID, n.ID] mod 256.
// Equivale a succ(clave) = n.ID (convencion Chord estandar).
// Usar la función auxiliar entre.
func (n *NodoChord) EsResponsable(clave int) bool {
	// COMPLETAR
	n.mu.RLock()
	defer n.mu.RUnlock()
	return entre(clave, n.PredecesorID, n.ID)
}

// TODO 10: Implementar MejorSalto.
// Busca en la FingerTable el nodo mas cercano que preceda a la clave.
// Retorna la Direccion del mejor nodo (o el sucesor si no hay mejor).
func (n *NodoChord) MejorSalto(clave int) string {
	// COMPLETAR
	n.mu.RLock()
	defer n.mu.RUnlock()
	for i := 2; i >= 0; i-- {
		fingerID := n.FingerTableIDs[i]
		if n.ID < clave {
			if fingerID > n.ID && fingerID < clave {
				return n.FingerTable[i]
			}
		} else if n.ID > clave {
			if fingerID > n.ID || fingerID < clave {
				return n.FingerTable[i]
			}
		}
	}
	return n.Sucesor
}

// TODO 11: Implementar Almacenar y Obtener.
// Almacenar guarda un par clave-valor localmente (protegido con mutex).
// Obtener recupera un valor por clave. Retorna el valor y un bool indicando si existe.
func (n *NodoChord) Almacenar(clave int, valor string) {
	// COMPLETAR
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Datos[clave] = valor
}

func (n *NodoChord) Obtener(clave int) (string, bool) {
	// STUB: retorna vacío y false
	n.mu.RLock()
	defer n.mu.RUnlock()
	val, ok := n.Datos[clave]
	return val, ok
}

// TODO 12: Implementar entre.
// Función auxiliar: retorna true si valor esta en (inicio, fin] (modulo 256).
func entre(valor, inicio, fin int) bool {
	// COMPLETAR
	if inicio < fin {
		return valor > inicio && valor <= fin
	} else if inicio > fin {
		return valor > inicio || valor <= fin
	}
	return true // inicio == fin, un solo nodo responsable de todo
}

// --- Servicio RPC para forwarding en cadena (Chord puro) ---

// ArgsStore es la solicitud RPC para almacenar una clave-valor.
type ArgsStore struct {
	Clave int
	Valor string
}

// RespStore es la respuesta RPC de almacenamiento.
type RespStore struct {
	NodoID          int
	NodoResponsable string
}

// ArgsLookup es la solicitud RPC para buscar una clave.
type ArgsLookup struct {
	Clave int
}

// RespLookup es la respuesta RPC de búsqueda.
type RespLookup struct {
	Valor           string
	Encontrado      bool
	NodoID          int
	NodoResponsable string
}

// ServicioChord expone métodos RPC que hacen forwarding en cadena
// usando la finger table (MejorSalto) cuando el nodo no es responsable.
type ServicioChord struct {
	Nodo *NodoChord
}

// TODO 13: Implementar Almacenar (RPC).
// Si el nodo es responsable, guardar localmente; si no, forwardea via
// MejorSalto por RPC al siguiente nodo en la cadena.
func (s *ServicioChord) Almacenar(args ArgsStore, resp *RespStore) error {
	// COMPLETAR
	if s.Nodo.EsResponsable(args.Clave) {
		s.Nodo.Almacenar(args.Clave, args.Valor)
		resp.NodoID = s.Nodo.ID
		resp.NodoResponsable = s.Nodo.Direccion
		return nil
	}
	siguiente := s.Nodo.MejorSalto(args.Clave)
	cliente, err := rpc.DialHTTP("tcp", siguiente)
	if err != nil {
		return err
	}
	defer cliente.Close()
	return cliente.Call("ServicioChord.Almacenar", args, resp)
}

// TODO 14: Implementar Obtener (RPC).
// Si el nodo es responsable, devolver local; si no, forwardea via MejorSalto.
func (s *ServicioChord) Obtener(args ArgsLookup, resp *RespLookup) error {
	// COMPLETAR
	if s.Nodo.EsResponsable(args.Clave) {
		val, ok := s.Nodo.Obtener(args.Clave)
		resp.Valor = val
		resp.Encontrado = ok
		resp.NodoID = s.Nodo.ID
		resp.NodoResponsable = s.Nodo.Direccion
		return nil
	}
	siguiente := s.Nodo.MejorSalto(args.Clave)
	cliente, err := rpc.DialHTTP("tcp", siguiente)
	if err != nil {
		return err
	}
	defer cliente.Close()
	return cliente.Call("ServicioChord.Obtener", args, resp)
}
