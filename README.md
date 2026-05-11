[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/A6uVSc3Y)

# Exercício 09 – Kafka + gRPC

Sistema que combina **pub-sub (Kafka)** e **cliente-servidor (gRPC)** para coletar leituras de sensores de temperatura, processar estatísticas em janela deslizante e expor os dados a clientes via web service.

## Arquitetura

```
(1) producer.py          (2) processor.py          (3) service.py          (4) client.py
  Sensor simulado    →      Consumidor/Produtor   →   Consumidor/gRPC    ↔   Cliente gRPC
  publica leituras          calcula média (2 h)        armazena em memória
  no Kafka                  publica estatísticas       expõe API gRPC
  [temperature-raw]         [temperature-processed]
```

### Tópicos Kafka

| Tópico                | Produzido por  | Consumido por  | Conteúdo                                   |
|-----------------------|----------------|----------------|--------------------------------------------|
| `temperature-raw`     | `producer.py`  | `processor.py` | leitura bruta: sensor_id, temperature, ts  |
| `temperature-processed`| `processor.py`| `service.py`   | snapshot: avg/min/max, janela, n_amostras  |

### Serviço gRPC (`TemperatureService`)

| RPC               | Argumento       | Retorno          | Descrição                                    |
|-------------------|-----------------|------------------|----------------------------------------------|
| `GetLatestReading`| `SensorRequest` | `ProcessedReading` | Última leitura processada do sensor         |
| `GetHistory`      | `SensorRequest` | `ReadingHistory` | Histórico completo de snapshots do sensor    |
| `ListSensors`     | `EmptyMessage`  | `SensorList`     | IDs de todos os sensores registrados         |
| `GetOverallStats` | `SensorRequest` | `OverallStats`   | Estatísticas globais (min/max/avg agregados) |

## Pré-requisitos

```bash
pip install kafka-python grpcio grpcio-tools
```

## Como executar (4 terminais)

```bash
# Terminal 1 – sensor (produtor)
python3 producer.py

# Terminal 2 – processador (consumidor/produtor)
python3 processor.py

# Terminal 3 – serviço gRPC (consumidor + servidor)
python3 service.py

# Terminal 4 – cliente gRPC
python3 client.py [sensor_id]   # padrão: sensor-A
```

## Regenerar arquivos protobuf

Caso altere `protos/TemperatureService.proto`:

```bash
python3 generate_protos.py
```

## Configuração

Edite `const.py` para ajustar endereços:

```python
BROKER_ADDR = '172.31.91.151'   # endereço do broker Kafka
BROKER_PORT = '9092'
GRPC_HOST   = 'localhost'       # host do serviço gRPC
GRPC_PORT   = '50052'
```

## Fluxo de dados

1. `producer.py` simula três sensores (`sensor-A/B/C`). Publica no Kafka apenas quando a variação supera 0,5 °C.
2. `processor.py` mantém uma janela deslizante de 2 horas por sensor. A cada novo evento publica um snapshot com média, mínimo e máximo da janela.
3. `service.py` consome os snapshots em thread background e os guarda em memória. O servidor gRPC responde consultas a partir desse estado.
4. `client.py` conecta ao servidor gRPC e demonstra todas as operações disponíveis.

