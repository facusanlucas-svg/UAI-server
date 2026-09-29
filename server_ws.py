# -*- coding: utf-8 -*-
"""
SERVIDOR MULTIJUGADOR DE UAIBOT (WebSockets por Internet)
==============================================

El servidor es el intermediario: los jugadores NUNCA se conectan entre si,
asi que no hace falta abrir puertos (port forwarding) en las casas de los jugadores.

Que hace:
  - Acepta conexiones WebSocket desde Internet en /ws.
  - Ofrece un health-check HTTP en /.
  - Crea salas con un CODIGO unico de 6 numeros.
  - Deja que otro jugador se una con ese codigo.
  - Mantiene muchas salas al mismo tiempo, cada una separada de las otras.
  - Reenvia los mensajes del jugador A al B y del B al A (sin cambiarlos).
  - Avisa cuando un jugador se desconecta.
  - Guarda en memoria las invitaciones entre amigos (duran 90 segundos).

Formato de los mensajes: JSON en formato string (framing por WebSocket).

COMO EJECUTARLO:
    python server_ws.py                 (escucha en el puerto 5560)
    (en servicios cloud que dan la variable PORT, la usa sola)
"""
import argparse
import asyncio
import http
import json
import os
import random
import time

import websockets

MAX_JUGADORES_POR_SALA = 2          # el juego online es 1 contra 1
MAX_LARGO_MENSAJE = 64 * 1024       # 64 KB por mensaje como maximo
DURACION_INVITACION = 90            # segundos
SALA_VACIA_MAXIMO = 2 * 60 * 60     # una sala esperando rival se borra a las 2 horas
TIEMPO_SIN_DATOS = 10 * 60          # (timeout)

COMANDOS_SERVIDOR = {
    "CREAR_SALA", "UNIRSE_SALA", "SALIR_SALA", "LISTAR_SALAS",
    "INVITAR", "VER_INVITACIONES", "BORRAR_INVITACIONES", "PING",
}


def log(*partes):
    """Escribe un mensaje con la hora en la consola del servidor."""
    print(time.strftime("[%H:%M:%S]"), *partes, flush=True)


class Cliente:
    """Un jugador conectado al servidor WS."""
    def __init__(self, ws):
        self.ws = ws
        self.nombre = "Jugador"
        self.sala = None
        self.direccion = str(ws.remote_address)

    async def enviar(self, mensaje):
        """Manda un mensaje (diccionario o string) a este jugador."""
        try:
            if isinstance(mensaje, dict):
                linea = json.dumps(mensaje, ensure_ascii=False)
            else:
                linea = mensaje
            if isinstance(linea, bytes):
                linea = linea.decode('utf-8')
            linea = linea.strip()
            await self.ws.send(linea)
        except Exception:
            pass


class Sala:
    """Una partida: un codigo y los jugadores que estan adentro."""
    def __init__(self, codigo, anfitrion, modo, publica):
        self.codigo = codigo
        self.anfitrion = anfitrion
        self.modo = modo
        self.publica = publica
        self.jugadores = [anfitrion]
        self.creada = time.time()

    def llena(self):
        return len(self.jugadores) >= MAX_JUGADORES_POR_SALA


class Servidor:
    """Guarda todas las salas e invitaciones y atiende a los jugadores."""
    def __init__(self):
        self.salas = {}          # codigo -> Sala
        self.invitaciones = {}   # usuario (minusculas) -> [invitacion, ...]

    # ------------------------------------------------------------ salas
    def _codigo_nuevo(self):
        """Genera un codigo de 6 numeros que no este en uso."""
        while True:
            codigo = str(random.randint(100000, 999999))
            if codigo not in self.salas:
                return codigo

    async def salir_de_sala(self, cliente, avisar=True):
        """Saca al jugador de su sala. Si queda alguien, le avisa que se fue."""
        sala = cliente.sala
        if not sala:
            return
        cliente.sala = None
        if cliente in sala.jugadores:
            sala.jugadores.remove(cliente)
        if avisar:
            for otro in sala.jugadores:
                await otro.enviar({"tipo": "DESCONECTADO", "nombre": cliente.nombre})
        # Una sala sin jugadores, o sin anfitrion esperando, se borra
        if not sala.jugadores or cliente is sala.anfitrion:
            for otro in list(sala.jugadores):
                otro.sala = None
            self.salas.pop(sala.codigo, None)
            log(f"Sala {sala.codigo} cerrada")

    async def crear_sala(self, cliente, msg):
        await self.salir_de_sala(cliente)
        cliente.nombre = str(msg.get("nombre") or cliente.nombre)[:30]
        modo = str(msg.get("modo") or "1v1")
        sala = Sala(self._codigo_nuevo(), cliente, modo, bool(msg.get("publica", False)))
        self.salas[sala.codigo] = sala
        cliente.sala = sala
        log(f"Sala {sala.codigo} creada por {cliente.nombre} ({modo}, {'publica' if sala.publica else 'privada'})")
        await cliente.enviar({"tipo": "SALA_CREADA", "codigo": sala.codigo, "modo": modo})

    async def unirse_sala(self, cliente, msg):
        codigo = str(msg.get("codigo") or "").replace("#", "").strip()
        sala = self.salas.get(codigo)
        if not sala:
            await cliente.enviar({"tipo": "ERROR", "mensaje": f"La sala #{codigo} no existe."})
            return
        if sala.llena():
            await cliente.enviar({"tipo": "ERROR", "mensaje": f"La sala #{codigo} ya esta llena."})
            return
        if cliente.sala is sala:
            return
        await self.salir_de_sala(cliente)
        cliente.nombre = str(msg.get("nombre") or cliente.nombre)[:30]
        sala.jugadores.append(cliente)
        cliente.sala = sala
        log(f"{cliente.nombre} entro a la sala {codigo}")
        await cliente.enviar({"tipo": "UNIDO", "codigo": codigo, "modo": sala.modo, "rival": sala.anfitrion.nombre})
        for otro in sala.jugadores:
            if otro is not cliente:
                await otro.enviar({"tipo": "RIVAL_CONECTADO", "nombre": cliente.nombre})

    def lista_publica(self):
        """Salas publicas que todavia esperan rival."""
        return [{"codigo": s.codigo, "nombre": s.anfitrion.nombre, "modo": s.modo}
                for s in self.salas.values() if s.publica and not s.llena()]

    # ------------------------------------------------------ invitaciones
    def _limpiar_invitaciones_viejas(self):
        ahora = time.time()
        for usuario in list(self.invitaciones):
            vivas = [i for i in self.invitaciones[usuario] if ahora - i["timestamp"] < DURACION_INVITACION]
            if vivas:
                self.invitaciones[usuario] = vivas
            else:
                del self.invitaciones[usuario]

    # ------------------------------------------------------ mensajes
    async def procesar(self, cliente, linea):
        """Decide que hacer con una linea que mando un jugador."""
        try:
            msg = json.loads(linea)
            if not isinstance(msg, dict):
                raise ValueError
        except Exception:
            await cliente.enviar({"tipo": "ERROR", "mensaje": "Mensaje invalido (no es JSON)."})
            return

        tipo = msg.get("tipo")
        if tipo not in COMANDOS_SERVIDOR:
            # Mensaje del juego: reenviarlo tal cual al/los rival(es) de la sala
            sala = cliente.sala
            if sala:
                for otro in sala.jugadores:
                    if otro is not cliente:
                        await otro.enviar(linea)
            return

        if tipo == "PING":
            await cliente.enviar({"tipo": "PONG"})
        elif tipo == "CREAR_SALA":
            await self.crear_sala(cliente, msg)
        elif tipo == "UNIRSE_SALA":
            await self.unirse_sala(cliente, msg)
        elif tipo == "SALIR_SALA":
            await self.salir_de_sala(cliente)
        elif tipo == "LISTAR_SALAS":
            await cliente.enviar({"tipo": "LISTA_SALAS", "salas": self.lista_publica()})
        elif tipo == "INVITAR":
            para = str(msg.get("para") or "").strip().lower()
            if para:
                self._limpiar_invitaciones_viejas()
                self.invitaciones.setdefault(para, []).append({
                    "de": str(msg.get("de") or cliente.nombre)[:30],
                    "codigo": str(msg.get("codigo") or ""),
                    "modo": str(msg.get("modo") or "1v1"),
                    "timestamp": time.time(),
                })
                log(f"Invitacion de {msg.get('de')} para {para} a la sala {msg.get('codigo')}")
            await cliente.enviar({"tipo": "INVITACION_ENVIADA"})
        elif tipo == "VER_INVITACIONES":
            self._limpiar_invitaciones_viejas()
            usuario = str(msg.get("usuario") or "").strip().lower()
            # solo las invitaciones a salas que siguen abiertas
            lista = [i for i in self.invitaciones.get(usuario, []) if i["codigo"] in self.salas]
            await cliente.enviar({"tipo": "INVITACIONES", "lista": lista})
        elif tipo == "BORRAR_INVITACIONES":
            self.invitaciones.pop(str(msg.get("usuario") or "").strip().lower(), None)
            await cliente.enviar({"tipo": "INVITACIONES_BORRADAS"})

    async def atender(self, ws, path="/"):
        """Atiende a un jugador desde que se conecta hasta que se va."""
        cliente = Cliente(ws)
        log(f"Conexion nueva desde {cliente.direccion} (path: {path})")
        try:
            async for mensaje in ws:
                if isinstance(mensaje, bytes):
                    mensaje = mensaje.decode("utf-8")
                mensaje = mensaje.strip()
                if mensaje:
                    await self.procesar(cliente, mensaje)
        except websockets.exceptions.ConnectionClosed:
            pass
        except Exception as e:
            log(f"Error con {cliente.direccion}: {e!r}")
        finally:
            await self.salir_de_sala(cliente)
            log(f"Desconectado {cliente.nombre} ({cliente.direccion})")

    async def limpieza_periodica(self):
        """Cada minuto borra salas abandonadas e invitaciones vencidas."""
        while True:
            await asyncio.sleep(60)
            ahora = time.time()
            for codigo, sala in list(self.salas.items()):
                if not sala.llena() and ahora - sala.creada > SALA_VACIA_MAXIMO:
                    for j in list(sala.jugadores):
                        await j.enviar({"tipo": "ERROR", "mensaje": "La sala se cerro por inactividad."})
                        j.sala = None
                    self.salas.pop(codigo, None)
            self._limpiar_invitaciones_viejas()


async def health_check(path, request_headers):
    """Maneja el health check HTTP de Render y solo permite WS en /ws."""
    if path == "/":
        return http.HTTPStatus.OK, [], b"OK"
    if path == "/ws":
        return None
    return http.HTTPStatus.NOT_FOUND, [], b"Not Found"


async def principal(host, puerto):
    servidor = Servidor()
    asyncio.create_task(servidor.limpieza_periodica())
    
    start_server = websockets.serve(
        servidor.atender, 
        host, 
        puerto,
        process_request=health_check,
        max_size=MAX_LARGO_MENSAJE
    )
    
    log(f"Servidor WS UAIBOT escuchando en {host}:{puerto}  (Ctrl+C para cerrar)")
    async with start_server:
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Servidor multijugador WS de UAIBOT")
    p.add_argument("--host", default="0.0.0.0", help="0.0.0.0 = aceptar conexiones de cualquier red")
    p.add_argument("--puerto", type=int, default=int(os.environ.get("PORT", 5560)))
    a = p.parse_args()
    try:
        asyncio.run(principal(a.host, a.puerto))
    except KeyboardInterrupt:
        log("Servidor cerrado")
