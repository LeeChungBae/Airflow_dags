# Airflow_dags

PLAYDATA 데이터엔지니어링 부트캠프 32기 팀 프로젝트 1팀 LeeChungBae의 airflow 전용 DAG (Directed Acyclic Graph) 레포지토리 입니다.

본 DAGs는 영화 박스오피스 데이터 수집(Extract)/처리(Transform)/저장(Ready-to-Load)를 통해 활용 가능한 데이터 뱅크를 만드는 것을 목표로 하며, 이를 위해 크게 extract DAG 3개, transform DAG 3개, load DAG 3개를 가지고 있습니다.

본 팀의 경우, 할당받은 박스오피스 데이터 년도는 2023년입니다.

## Installation

다음 코드를 통해 레포지토리를 clone 합니다.
```bash
$ git clone git@github.com:LeeChungBae/Airflow_dags.git
```

이후 해당 레포지토리로 이동해 절대경로를 확인합니다.
```bash
$ cd <CLONED_REPOSITORY>
$ pwd
```
이 때 출력된 해당 절대 경로를 `<PATH>` 라 할 때, `AIRFLOW HOME` 디렉토리의 `airflow.cfg` 파일의 `dags_folder` 값을  다음과 같이 수정합니다.
```bash
# Variable: AIRFLOW__CORE__DAGS_FOLDER
#
dags_folder = <PATH>
```
이후, `airflow standalone` 으로 에어플로우 서버를 재시작할 시 DAG들은 해당 레포지토리에서부터 읽어지게 됩니다.

## DAGs 별 기능
- `extract1.py`, `extract2.py`, `extract3.py`
 : 영화 박스오피스 데이터의 수집을 위한 DAG 파일들로, 공통적으로 `extract_package` 패키지를 사용합니다. 각각의 파일은 2023년을 3개 분기로 나누어 1 - 4월, 5 - 8월, 9 - 12월의 데이터를 각각 수집해 지정된 경로에 일차적으로 저장합니다. 

- `transform1.py`, `transform2.py`, `transform3.py`
 : 직전 3개 DAG에서 수집한 데이터들을 가공해 다시 저장하는 DAG들로, 공통적으로 `transform_package` 패키지를 사용합니다. 데이터들은 `extract` DAG들이 각각  나누어 저장한 것을 그대로 이용합니다. 

- `load1.py`, `load2.py`, `load3.py`
: `transform` DAG에서 처리한 데이터를 행을 구별해 `parquet` 형식으로 최종적으로 저장하는 DAG입니다. 공통적으로 `load_package` 패키지를 사용합니다.
