# Simulador de rede de filas

Este projeto implementa uma simulação de eventos discretos para redes abertas de filas com qualquer quantidade de filas, servidores, capacidades e rotas probabilísticas. A entrada é um arquivo `.yml`; o programa não exige bibliotecas externas.

## Como executar

Com Python 3.10 ou superior, na pasta do projeto:

```bash
python3 src/simulador.py config/modelo_t1.yml --json resultado_t1.json
```

O relatório é exibido na tela e o parâmetro `--json` salva os mesmos resultados em formato estruturado.

## Convenções do modelo

- `capacity` é a capacidade total da fila, isto é, inclui os clientes sendo atendidos. Assim, `G/G/2/5` admite no máximo cinco clientes no sistema, dos quais até dois estão em serviço.
- `capacity: unlimited` representa capacidade ilimitada.
- As filas usam disciplina FIFO e os atendimentos são não preemptivos.
- As distribuições são uniformes contínuas. Para cada duração, é usado um número pseudoaleatório `U` e calculado `min + U * (max - min)`.
- O primeiro cliente externo chega no instante indicado em `first_external_arrival`; as chegadas seguintes usam a distribuição em `external_arrival`.
- Em uma chegada externa, o próximo intervalo entre chegadas é sorteado antes de iniciar o atendimento do cliente recém-chegado. Em uma conclusão, o destino é sorteado antes de eventual início de atendimento na fila de destino.
- O limite conta cada sorteio de intervalo entre chegadas, tempo de atendimento ou roteamento. Ao consumir o último aleatório permitido, a simulação encerra no instante do evento que o consumiu; os estados são acumulados até esse instante.

## Modelo entregue

O arquivo `config/modelo_t1.yml` representa o diagrama da atividade:

- Fila 1: um servidor, capacidade ilimitada, atendimento entre 1 e 2 min;
- Fila 2: dois servidores, capacidade total 5, atendimento entre 4 e 6 min;
- Fila 3: dois servidores, capacidade total 10, atendimento entre 5 e 15 min;
- chegadas externas na fila 1 entre 2 e 4 min, com a primeira chegada em 2,0 min;
- rotas conforme as probabilidades do diagrama.

O valor `seed: 20260929` torna a execução reproduzível. Trocar a semente altera os valores numéricos, mas não o funcionamento do simulador.
# t1-simulador
