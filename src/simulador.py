from __future__ import annotations

import argparse
import heapq
import json
import random
import sys
from collections import Counter, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


def _scalar(value: str) -> Any:
    """Converte os valores simples que o formato de configuração utiliza."""
    value = value.strip()
    if not value:
        return None
    if (value.startswith("\"") and value.endswith("\"")) or (
        value.startswith("'") and value.endswith("'")
    ):
        return value[1:-1]
    lowered = value.lower()
    if lowered in {"null", "none"}:
        return None
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value


def load_simple_yaml(path: Path) -> dict[str, Any]:
    """Lê o subconjunto de YAML usado pelos modelos deste projeto.

    Ele suporta mapas e listas identadas, comentários e valores escalares. Para
    configurações mais elaboradas, basta manter o mesmo conjunto de estruturas.
    """
    lines: list[tuple[int, str]] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        content = raw.split("#", 1)[0].rstrip()
        if not content.strip():
            continue
        indent = len(content) - len(content.lstrip(" "))
        if "\t" in raw[: len(raw) - len(raw.lstrip())]:
            raise ValueError("Use espaços, não tabulações, no arquivo YAML.")
        lines.append((indent, content.strip()))

    def parse_block(index: int, indent: int) -> tuple[Any, int]:
        if index >= len(lines) or lines[index][0] < indent:
            return {}, index
        is_list = lines[index][1].startswith("- ")
        if is_list:
            result: list[Any] = []
            while index < len(lines) and lines[index][0] == indent and lines[index][1].startswith("- "):
                item = lines[index][1][2:].strip()
                index += 1
                if ":" not in item:
                    result.append(_scalar(item))
                    continue
                key, value = item.split(":", 1)
                mapping: dict[str, Any] = {key.strip(): _scalar(value)}
                if index < len(lines) and lines[index][0] > indent:
                    child_indent = lines[index][0]
                    child, index = parse_block(index, child_indent)
                    if not isinstance(child, dict):
                        raise ValueError("Cada item de lista deve conter um mapa.")
                    mapping.update(child)
                result.append(mapping)
            return result, index

        result = {}
        while index < len(lines) and lines[index][0] == indent and not lines[index][1].startswith("- "):
            item = lines[index][1]
            if ":" not in item:
                raise ValueError(f"Linha YAML inválida: {item!r}")
            key, value = item.split(":", 1)
            key, value = key.strip(), value.strip()
            index += 1
            if value:
                result[key] = _scalar(value)
            elif index < len(lines) and lines[index][0] > indent:
                result[key], index = parse_block(index, lines[index][0])
            else:
                result[key] = {}
        return result, index

    parsed, end = parse_block(0, lines[0][0] if lines else 0)
    if end != len(lines) or not isinstance(parsed, dict):
        raise ValueError("A raiz do YAML deve ser um mapa.")
    return parsed


@dataclass
class QueueState:
    name: str
    servers: int
    capacity: int | None
    service_min: float
    service_max: float
    waiting: deque[int] = field(default_factory=deque)
    in_service: dict[int, int] = field(default_factory=dict)
    accumulated_time: Counter[int] = field(default_factory=Counter)
    last_change: float = 0.0
    losses: int = 0

    @property
    def population(self) -> int:
        return len(self.waiting) + len(self.in_service)

    def add_elapsed_time(self, now: float) -> None:
        self.accumulated_time[self.population] += now - self.last_change
        self.last_change = now


class NetworkSimulator:
    def __init__(self, model: dict[str, Any]) -> None:
        self.model = model
        simulation = model["simulation"]
        self.random_limit = int(simulation["random_limit"])
        self.rng = random.Random(simulation["seed"])
        self.random_used = 0
        self.now = 0.0
        self.halted = False
        self.event_number = 0
        self.customer_number = 0
        self.events: list[tuple[float, int, int, str, str | None, int | None]] = []
        self.queues: dict[str, QueueState] = {}
        for name, spec in model["queues"].items():
            capacity_value = spec["capacity"]
            capacity = None if str(capacity_value).lower() == "unlimited" else int(capacity_value)
            service = spec["service"]
            if service["distribution"] != "uniform":
                raise ValueError("Este simulador implementa a distribuição uniforme.")
            self.queues[name] = QueueState(
                name=name,
                servers=int(spec["servers"]),
                capacity=capacity,
                service_min=float(service["min"]),
                service_max=float(service["max"]),
            )
        self.routing = model["routing"]
        self.external = model["external_arrival"]
        self._validate_model()

    def _validate_model(self) -> None:
        if self.random_limit <= 0:
            raise ValueError("random_limit deve ser positivo.")
        if self.external["queue"] not in self.queues:
            raise ValueError("A fila de chegada externa não existe.")
        if self.external["distribution"] != "uniform":
            raise ValueError("Este simulador implementa a distribuição uniforme.")
        for origin, routes in self.routing.items():
            if origin not in self.queues:
                raise ValueError(f"Roteamento definido para fila inexistente: {origin}")
            total = sum(float(route["probability"]) for route in routes)
            if abs(total - 1.0) > 1e-9:
                raise ValueError(f"As probabilidades de {origin} devem somar 1; soma atual: {total}.")
            for route in routes:
                if route["destination"] != "exit" and route["destination"] not in self.queues:
                    raise ValueError(f"Destino inexistente: {route['destination']}")

    def _random(self) -> float | None:
        if self.random_used >= self.random_limit:
            self.halted = True
            return None
        self.random_used += 1
        return self.rng.random()

    def _uniform(self, low: float, high: float) -> float | None:
        value = self._random()
        return None if value is None else low + value * (high - low)

    def _schedule(self, when: float, priority: int, event: str, queue: str | None, customer: int | None) -> None:
        self.event_number += 1
        heapq.heappush(self.events, (when, priority, self.event_number, event, queue, customer))

    def _start_waiting_customers(self, queue: QueueState) -> None:
        while queue.waiting and len(queue.in_service) < queue.servers and not self.halted:
            customer = queue.waiting.popleft()
            duration = self._uniform(queue.service_min, queue.service_max)
            if duration is None:
                queue.waiting.appendleft(customer)
                return
            server = next(server for server in range(queue.servers) if server not in queue.in_service)
            queue.in_service[server] = customer
            self._schedule(self.now + duration, 0, "completion", queue.name, server)

    def _arrive(self, queue_name: str, customer: int) -> None:
        queue = self.queues[queue_name]
        if queue.capacity is not None and queue.population >= queue.capacity:
            queue.losses += 1
            return
        queue.add_elapsed_time(self.now)
        queue.waiting.append(customer)
        self._start_waiting_customers(queue)

    def _route(self, origin: str) -> str | None:
        draw = self._random()
        if draw is None:
            return None
        cumulative = 0.0
        for route in self.routing[origin]:
            cumulative += float(route["probability"])
            if draw < cumulative:
                return str(route["destination"])
        return str(self.routing[origin][-1]["destination"])

    def _handle_external_arrival(self) -> None:
        self.customer_number += 1
        customer = self.customer_number

        interval = self._uniform(float(self.external["min"]), float(self.external["max"]))
        if interval is not None:
            self._schedule(self.now + interval, 1, "external", None, None)
        self._arrive(str(self.external["queue"]), customer)

    def _handle_completion(self, queue_name: str, server: int) -> None:
        queue = self.queues[queue_name]
        queue.add_elapsed_time(self.now)
        customer = queue.in_service.pop(server)
        destination = self._route(queue_name)
        if destination is None:
            return
        if destination != "exit":
            self._arrive(destination, customer)
        self._start_waiting_customers(queue)

    def run(self) -> dict[str, Any]:
        first_arrival = float(self.model["simulation"]["first_external_arrival"])
        self._schedule(first_arrival, 1, "external", None, None)
        while self.events and not self.halted:
            when, _priority, _order, event, queue, item = heapq.heappop(self.events)
            self.now = when
            if event == "external":
                self._handle_external_arrival()
            else:
                assert queue is not None and item is not None
                self._handle_completion(queue, item)

        for queue in self.queues.values():
            queue.add_elapsed_time(self.now)

        return {
            "simulation_time": self.now,
            "random_numbers_used": self.random_used,
            "stop_reason": "random_limit_reached" if self.halted else "event_list_empty",
            "queues": {
                name: {
                    "losses": queue.losses,
                    "accumulated_time_by_state": {
                        str(state): queue.accumulated_time[state]
                        for state in sorted(queue.accumulated_time)
                    },
                    "probability_by_state": {
                        str(state): queue.accumulated_time[state] / self.now if self.now else 0.0
                        for state in sorted(queue.accumulated_time)
                    },
                }
                for name, queue in self.queues.items()
            },
        }


def format_report(result: dict[str, Any]) -> str:
    lines = [
        "RESULTADO DA SIMULACAO",
        f"Aleatorios utilizados: {result['random_numbers_used']}",
        f"Criterio de parada: {result['stop_reason']}",
        f"Tempo global: {result['simulation_time']:.6f} min",
    ]
    for name, queue in result["queues"].items():
        lines.extend(["", name.upper(), f"Perdas: {queue['losses']}", "Estado | Tempo acumulado (min) | Probabilidade"])
        for state, time in queue["accumulated_time_by_state"].items():
            probability = queue["probability_by_state"][state]
            lines.append(f"{state:>6} | {time:>21.6f} | {probability:.8f}")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Simula uma rede de filas configurada em YAML.")
    parser.add_argument("model", type=Path, help="caminho do arquivo .yml")
    parser.add_argument("--json", dest="json_path", type=Path, help="salva também o resultado em JSON")
    args = parser.parse_args()
    try:
        result = NetworkSimulator(load_simple_yaml(args.model)).run()
    except (KeyError, TypeError, ValueError) as error:
        print(f"Erro no modelo: {error}", file=sys.stderr)
        return 2
    print(format_report(result))
    if args.json_path:
        args.json_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
