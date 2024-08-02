# Airflow_dags

PLAYDATA 데이터엔지니어링 부트캠프 32기 팀 프로젝트 1팀 LeeChungBae의 airflow 전용 DAG (Directed Acyclic Graph) 레포지토리 입니다.

# Installation

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

# DAGs 별 기능
## `extract1`, `extract2`, `extract3`
todo

## `transform1`, `transform2`, `transform3`
todo

## `load1`, `load2`, `load3`
todo
