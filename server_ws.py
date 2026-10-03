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
  - Reenvia los mensajes entre los jugadores de la sala (1v1 o 2v2 con 4 jugadores).
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

MAX_JUGADORES_POR_SALA = 4          # 2 contra 2 (Equipo Azul y Equipo Rojo)
MAX_LARGO_MENSAJE = 64 * 1024       # 64 KB por mensaje como maximo
DURACION_INVITACION = 90            # segundos
SALA_VACIA_MAXIMO = 2 * 60 * 60     # una sala esperando rival se borra a las 2 horas
TIEMPO_SIN_DATOS = 10 * 60          # (timeout)

COMANDOS_SERVIDOR = {
    "CREAR_SALA", "UNIRSE_SALA", "SALIR_SALA", "LISTAR_SALAS",
    "INVITAR", "VER_INVITACIONES", "BORRAR_INVITACIONES", "PING",
    "EN_VIVO", "ESPECTAR", "DEJAR_ESPECTAR", "REVANCHA",
}


def capacidad_para(modo):
    """Cuantos jugadores entran en la sala segun el modo ('1v1', '2v2:CRISTALES', ...)."""
    return MAX_JUGADORES_POR_SALA if str(modo).startswith("2v2") else 2


def es_modo_arena(modo):
    """Salas de arena ('2v2:...' o '1v1:...'): el servidor controla el orden de los turnos."""
    modo = str(modo)
    return ":" in modo or modo.startswith("2v2")


def equipo_de(num):
    """Jugadores 1 y 3 = Equipo Azul, 2 y 4 = Equipo Rojo."""
    return "AZUL" if num % 2 == 1 else "ROJO"


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
    """Una partida: un codigo y los jugadores que estan adentro.
    Cada jugador tiene un puesto (1..4): 1 y 3 Equipo Azul, 2 y 4 Equipo Rojo.
    El 1 (anfitrion) es el capitan azul y el 2 el capitan rojo."""
    def __init__(self, codigo, anfitrion, modo, publica):
        self.codigo = codigo
        self.anfitrion = anfitrion
        self.modo = modo
        self.publica = publica
        self.jugadores = [anfitrion]
        self.puestos = {1: anfitrion}
        anfitrion.num = 1
        self.capacidad = capacidad_para(modo)
        self.creada = time.time()
        self.empezada = False       # ya se mando SALA_LISTA / SALA_2V2_LISTA
        self.turno_n = 0            # numero de la proxima accion de turno esperada
        self.aviso = None           # SALA_LISTA de la partida en curso (para espectadores)
        self.historial = []         # acciones de la partida en curso (para espectadores)
        self.espectadores = []      # clientes mirando la partida
        self.revancha = set()       # puestos que pidieron revancha

    def llena(self):
        return len(self.jugadores) >= self.capacidad

    def puesto_libre(self):
        for n in range(1, self.capacidad + 1):
            if n not in self.puestos:
                return n
        return None

    def lista_jugadores(self):
        return [{"num": n, "nombre": c.nombre, "equipo": equipo_de(n)} for n, c in sorted(self.puestos.items())]


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

    async def dejar_espectar(self, cliente):
        sala = getattr(cliente, "espectando", None)
        if sala and cliente in sala.espectadores:
            sala.espectadores.remove(cliente)
        cliente.espectando = None

    async def salir_de_sala(self, cliente, avisar=True):
        """Saca al jugador de su sala. Si queda alguien, le avisa que se fue."""
        await self.dejar_espectar(cliente)
        sala = cliente.sala
        if not sala:
            return
        cliente.sala = None
        num = getattr(cliente, "num", 0)
        if cliente in sala.jugadores:
            sala.jugadores.remove(cliente)
        if sala.puestos.get(num) is cliente:
            del sala.puestos[num]
        sala.revancha.discard(num)
        if avisar:
            for otro in sala.jugadores + sala.espectadores:
                await otro.enviar({"tipo": "DESCONECTADO", "nombre": cliente.nombre,
                                   "jugador_num": num, "jugadores": sala.lista_jugadores()})
        # Una sala sin jugadores, o sin anfitrion esperando, se borra
        if not sala.jugadores or cliente is sala.anfitrion:
            for otro in list(sala.jugadores):
                otro.sala = None
            for esp in list(sala.espectadores):
                await esp.enviar({"tipo": "DESCONECTADO", "nombre": "", "jugador_num": 1, "jugadores": []})
                esp.espectando = None
            sala.espectadores = []
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
        await cliente.enviar({"tipo": "SALA_CREADA", "codigo": sala.codigo, "modo": modo,
                              "capacidad": sala.capacidad, "jugador_num": 1,
                              "jugadores": sala.lista_jugadores()})

    async def unirse_sala(self, cliente, msg):
        codigo = str(msg.get("codigo") or "").replace("#", "").strip()
        sala = self.salas.get(codigo)
        if not sala:
            await cliente.enviar({"tipo": "ERROR", "mensaje": f"La sala #{codigo} no existe."})
            return
        if sala.llena() or sala.empezada:
            await cliente.enviar({"tipo": "ERROR", "mensaje": f"La sala #{codigo} ya esta llena."})
            return
        if cliente.sala is sala:
            return
        await self.salir_de_sala(cliente)
        cliente.nombre = str(msg.get("nombre") or cliente.nombre)[:30]
        num = sala.puesto_libre()
        cliente.num = num
        sala.puestos[num] = cliente
        sala.jugadores.append(cliente)
        cliente.sala = sala
        log(f"{cliente.nombre} entro a la sala {codigo} (puesto {num}, {equipo_de(num)})")
        await cliente.enviar({"tipo": "UNIDO", "codigo": codigo, "modo": sala.modo, "rival": sala.anfitrion.nombre,
                              "jugador_num": num, "capacidad": sala.capacidad,
                              "jugadores": sala.lista_jugadores()})
        for otro in sala.jugadores:
            if otro is not cliente:
                await otro.enviar({"tipo": "RIVAL_CONECTADO", "nombre": cliente.nombre,
                                   "jugador_num": num, "jugadores": sala.lista_jugadores()})
        # Tablero clasico (1v1): la partida empieza al entrar el rival (se guarda para espectadores)
        if sala.llena() and not es_modo_arena(sala.modo) and not sala.empezada:
            sala.empezada = True
            sala.aviso = {"tipo": "PARTIDA_CLASICA", "modo": sala.modo,
                          "jugadores": {str(n): c.nombre for n, c in sala.puestos.items()}}
            sala.historial = []
        # Sala de arena completa: se reparten los equipos y empieza la partida
        if sala.llena() and es_modo_arena(sala.modo) and not sala.empezada:
            sala.empezada = True
            sala.turno_n = 0
            nombres = {str(n): c.nombre for n, c in sala.puestos.items()}
            aviso = {
                "tipo": "SALA_2V2_LISTA" if sala.capacidad == 4 else "SALA_LISTA",
                "modo": sala.modo,
                "equipos": {"azul": [sala.puestos[n].nombre for n in sorted(sala.puestos) if n % 2 == 1],
                            "rojo": [sala.puestos[n].nombre for n in sorted(sala.puestos) if n % 2 == 0]},
                "jugadores": nombres,
                "semilla": random.randint(1, 10 ** 9),
            }
            log(f"Sala {codigo} lista: {aviso['equipos']}")
            sala.aviso = aviso
            sala.historial = []
            for c in sala.jugadores:
                await c.enviar(aviso)

    def lista_publica(self):
        """Salas publicas que todavia esperan rival."""
        return [{"codigo": s.codigo, "nombre": s.anfitrion.nombre, "modo": s.modo,
                 "jugadores": len(s.jugadores), "capacidad": s.capacidad}
                for s in self.salas.values() if s.publica and not s.llena() and not s.empezada]

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
            if sala and es_modo_arena(sala.modo) and tipo == "TURNO_ACCION" and "n" in msg:
                # Turnos sincronizados: solo pasa la accion que toca, del jugador que toca
                # (el anfitrion puede jugar por un compañero que se desconecto)
                try:
                    n = int(msg.get("n"))
                    jid = int(msg.get("jugador_id", 0))
                except (TypeError, ValueError):
                    n, jid = -1, 0
                esperado = (sala.turno_n % sala.capacidad) + 1
                permitido = getattr(cliente, "num", 0) == jid or cliente is sala.anfitrion
                if n != sala.turno_n or jid != esperado or not permitido:
                    await cliente.enviar({"tipo": "TURNO_RECHAZADO", "n_esperado": sala.turno_n,
                                          "jugador_esperado": esperado})
                    return
                if msg.get("fin_turno", True):
                    sala.turno_n += 1
                sala.historial.append(msg)
                for esp in list(sala.espectadores):
                    await esp.enviar(msg)
            elif sala and sala.aviso and tipo in ("TURNO_ACCION", "FIN_PARTIDA"):
                # tablero clasico: se guarda la jugada para los espectadores
                sala.historial.append(msg)
                for esp in list(sala.espectadores):
                    await esp.enviar(msg)
            if sala:
                for otro in sala.jugadores:
                    if otro is not cliente:
                        await otro.enviar(linea)
            return

        if tipo == "PING":
            await cliente.enviar({"tipo": "PONG"})
        elif tipo == "EN_VIVO":
            partidas = [{"codigo": s.codigo, "modo": s.modo,
                         "jugadores": [c.nombre for _n, c in sorted(s.puestos.items())],
                         "espectadores": len(s.espectadores), "acciones": len(s.historial)}
                        for s in self.salas.values() if s.aviso and s.empezada]
            await cliente.enviar({"tipo": "EN_VIVO", "partidas": partidas})
        elif tipo == "ESPECTAR":
            sala = self.salas.get(str(msg.get("codigo") or "").replace("#", "").strip())
            if not sala or not sala.aviso:
                await cliente.enviar({"tipo": "ERROR", "mensaje": "Esa partida ya no esta en vivo."})
                return
            await self.dejar_espectar(cliente)
            cliente.nombre = str(msg.get("nombre") or cliente.nombre)[:30]
            sala.espectadores.append(cliente)
            cliente.espectando = sala
            log(f"{cliente.nombre} mira la sala {sala.codigo}")
            await cliente.enviar({"tipo": "ESPECTANDO", "codigo": sala.codigo, "modo": sala.modo,
                                  "sala_lista": sala.aviso, "historial": list(sala.historial)})
        elif tipo == "DEJAR_ESPECTAR":
            await self.dejar_espectar(cliente)
        elif tipo == "REVANCHA":
            sala = cliente.sala
            if not sala or not sala.aviso:
                await cliente.enviar({"tipo": "ERROR", "mensaje": "No hay partida para revancha."})
                return
            sala.revancha.add(getattr(cliente, "num", 0))
            listos = len(sala.revancha)
            for c in sala.jugadores:
                await c.enviar({"tipo": "REVANCHA_ESTADO", "listos": listos, "total": sala.capacidad,
                                "de": cliente.nombre})
            if listos >= sala.capacidad and len(sala.jugadores) >= sala.capacidad:
                aviso = dict(sala.aviso)
                aviso["semilla"] = random.randint(1, 10 ** 9)
                aviso["revancha"] = True
                sala.aviso, sala.historial, sala.turno_n = aviso, [], 0
                sala.revancha = set()
                log(f"Revancha en la sala {sala.codigo}")
                for c in sala.jugadores:
                    await c.enviar(aviso if es_modo_arena(sala.modo) else {"tipo": "REVANCHA_LISTA"})
                for esp in list(sala.espectadores):
                    await esp.enviar({"tipo": "ESPECTANDO", "codigo": sala.codigo, "modo": sala.modo,
                                      "sala_lista": aviso, "historial": []})
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

    async def atender(self, ws, *args):
        """Atiende a un jugador desde que se conecta hasta que se va."""
        cliente = Cliente(ws)
        path = getattr(getattr(ws, "request", None), "path", "/")
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


def health_check(arg1, arg2=None):
    """Maneja el health check HTTP de Render y permite el handshake WebSocket.
    Compatible con websockets moderno (conn, req) y legado (path, headers)."""
    # API moderna de websockets (conn, req)
    if hasattr(arg1, "respond") and hasattr(arg2, "path"):
        if arg2.path == "/":
            return arg1.respond(http.HTTPStatus.OK, "OK\n")
        return None
    # API legada de websockets (path, headers)
    if arg1 == "/":
        return http.HTTPStatus.OK, [], b"OK\n"
    return None


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
