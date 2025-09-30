# BOAZ-25-kafka-practice

카프카 분산 처리 및 고가용성 실습 (BOAZ 25th)

본 저장소는 아파치 카프카(Apache Kafka)의 핵심 특징인 **확장성(Scalability)**과 **고가용성(High Availability)**을 직접 체험하고, 안정적인 데이터 처리를 위한 DLQ(Dead Letter Queue) 패턴을 구현하는 실습을 위해 만들어졌습니다.

## 🚀 실습 1: 카프카의 확장성과 고가용성 이해하기

### 🎯 실습 목표

- **확장성 (Scalability)**: 여러 컨슈머(Consumer)가 하나의 토픽(Topic) 데이터를 어떻게 분산 처리하는지 확인합니다.
- **고가용성 (High Availability)**: 실행 중인 컨슈머 중 하나에 장애가 발생했을 때, 다른 컨슈머가 작업을 이어받아 처리하는지 확인합니다.

### ✅ 1단계: 환경 구성 (Ubuntu 기준)

#### 0. Docker Desktop 실행
실습 전 Docker Desktop이 실행되고 있는지 확인해주세요.

#### 1. 실습 코드 다운로드 (Git Clone)
터미널을 열고, 실습 코드가 담긴 저장소를 아래 명령어로 다운로드합니다.

```bash
git clone https://github.com/vividbaek/BOAZ-25-kafka-practice.git
cd BOAZ-25-kafka-practice/
```

#### 2. 카프카 서버 실행 (Docker)
Docker를 이용해 카프카와 주키퍼(Zookeeper) 서버를 실행합니다.

```bash
docker-compose up -d
```

아래 명령어로 kafka와 zookeeper 두 컨테이너가 모두 Up 상태인지 확인합니다.

```bash
docker ps
```

#### 3. 파이썬 가상환경 준비
독립된 파이썬 환경을 구성하여 안정성을 높입니다.

```bash
# 파이썬 및 패키지 관리 도구 설치 (Ubuntu)
sudo apt update
sudo apt install python3 python3-pip

# 'kafka-env'라는 이름의 가상환경 생성
python3 -m venv kafka-env

# 가상환경 활성화
source kafka-env/bin/activate
# (터미널 앞에 (kafka-env) 표시가 나타납니다)

# kafka-python 라이브러리 설치
pip install kafka-python
```

### 🚀 2단계: 실습 진행

> 💡 **Tip**: 실습을 진행할 모든 터미널에서 `cd BOAZ-25-kafka-practice/` 폴더로 이동 후, `source kafka-env/bin/activate` 명령어로 가상환경을 활성화해주세요.

총 4개의 터미널을 준비합니다. (컨슈머 3개, 프로듀서 1개)

#### 1. 컨슈머 실행 (터미널 3개)
데이터를 처리할 컨슈머를 3개의 터미널에서 각각 실행합니다.

```bash
# [터미널 1]
python3 consumer.py

# [터미널 2]
python3 consumer.py

# [터미널 3]
python3 consumer.py
```

#### 2. 프로듀서 실행 (터미널 1개)
마지막 터미널에서 메시지를 생성하여 카프카로 보내는 프로듀서를 실행합니다.

```bash
# [터미널 4]
python3 producer.py
```

### 📊 3단계: 결과 확인 및 분석

#### 1. 확장성 (부하 분산) 확인
3개의 컨슈머 터미널을 확인하면, 프로듀서가 보낸 메시지가 3개의 컨슈머에 골고루 분산되어 출력되는 것을 볼 수 있습니다.

각 컨슈머 로그에 출력되는 Partition 번호가 서로 다른 것을 통해 이를 확인할 수 있습니다.

#### 2. 고가용성 (장애 대응) 확인
컨슈머가 실행 중인 터미널 중 하나를 `Ctrl+C`로 강제 종료합니다.

잠시 후, 남아있는 2개의 컨슈머가 종료된 컨슈머가 담당하던 파티션의 메시지를 자동으로 이어받아 처리하는 것을 볼 수 있습니다.

### [실습 과제 1]
컨슈머를 하나만 남을 때까지 종료해보세요. 마지막에 남은 컨슈머 하나가 모든 파티션(0, 1, 2)의 메시지를 처리하는 터미널 화면을 캡처하여 제출해주세요.

---

## 📝 과제: DLQ(Dead Letter Queue) 패턴으로 안정적인 컨슈머 만들기

### 🤔 DLQ(Dead Letter Queue)란?
DLQ는 "죽은 편지들이 모이는 곳"이라는 뜻으로, 데이터 처리 시스템에서 다양한 이유로 처리할 수 없는 '불량 메시지'를 격리하여 보관하는 특별한 장소(보통 별도의 토픽)를 말합니다. 불량 메시지 하나 때문에 전체 시스템이 멈추는 것을 방지하고, 데이터 유실 없이 원인을 분석하고 재처리할 기회를 제공합니다.

### 🎯 과제 목표
`producer.py`가 보내는 메시지 중 파싱할 수 없는 '불량 데이터'를 만나도 서비스가 멈추지 않도록 `consumer.py` 코드를 수정합니다. 처리 실패한 데이터는 별도의 DLQ 토픽으로 안전하게 격리하는 안정적인 컨슈머를 완성합니다.

### 🚀 미션
`assignment` 폴더로 이동하여 `consumer.py` 코드를 아래 요구사항에 맞게 수정해주세요.

1. **예외 처리**: `try-except` 구문을 사용하여 `json.JSONDecodeError`가 발생해도 프로그램이 종료되지 않도록 처리합니다.

2. **DLQ로 전송**: 예외가 발생하면, 처리 실패한 원본 메시지를 `event-stream-dlq` 라는 새로운 토픽으로 보내는 로직을 추가합니다.

### 💡 핵심 힌트:
- `consumer.py` 내부에 DLQ로 메시지를 보낼 `KafkaProducer`를 새로 생성해야 합니다.
- 정답 코드(`consumer-dlq-answer.py`)와 비교해보면, 주석 처리된 3줄을 활성화하는 것만으로도 구현이 가능합니다.

### ✅ 실행 및 결과 확인 방법

#### 1. 폴더 이동 및 환경 활성화
두 개의 터미널을 준비하고, 각각 `assignment` 폴더로 이동하여 가상환경을 활성화합니다.

```bash
cd assignment/
source ../kafka-env/bin/activate
```

#### 2. 프로듀서 및 수정한 컨슈머 실행

```bash
# [터미널 1] 문제 출제용 프로듀서 실행
python3 producer.py

# [터미널 2] 여러분이 수정한 컨슈머 실행
python3 consumer.py
```

#### 3. 성공 조건
컨슈머 터미널에서 불량 메시지를 만나도 멈추지 않고, DLQ 토픽으로 메시지를 전송합니다. `"DLQ로 메시지 전송: ..."`과 같은 로그를 남긴 후 다음 메시지를 계속 처리하면 성공입니다.

#### 4. 최종 확인
아래 명령어를 실행했을 때, `event-stream-dlq` 토픽에서 실패했던 불량 메시지들만 조회되면 완벽하게 성공한 것입니다.

```bash
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic event-stream-dlq --from-beginning
```

### [실습 과제 2]
수정한 컨슈머가 에러 데이터를 DLQ 토픽 `event-stream-dlq`로 보내는 로그가 출력된 터미널 화면을 캡처해서 제출해주세요.

---

## ✨ 정답 코드
과제 해결에 어려움이 있다면 `assignment/consumer-dlq-answer.py` 파일의 정답 코드를 참고하세요.