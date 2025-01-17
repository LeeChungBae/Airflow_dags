# Airflow_dags

PLAYDATA 데이터엔지니어링 부트캠프 32기 팀 LeeChungBae의 airflow 전용 DAG (Directed Acyclic Graph) 레포지토리 입니다.

본 DAGs는 영화 박스오피스 데이터 수집(Extract)/처리(Transform)/저장(Ready-to-Load)를 통해 활용 가능한 데이터 뱅크를 만드는 것을 목표로 하며, 이를 위해 크게 extract DAG 3개, transform DAG 3개, load DAG 3개를 가지고 있습니다.

본 팀의 경우, 할당받은 박스오피스 데이터 년도는 2023년입니다.

- **데이터 정제**: 수집된 박스오피스 데이터에서 불필요한 정보를 제거하고, 누락된 데이터를 처리합니다.
- **데이터 변환**: 정제된 데이터를 분석 및 시각화에 적합한 형식으로 변환합니다.
- **데이터 구조화**: 변환된 데이터를 데이터베이스에 저장하거나 다른 시스템으로 전송할 수 있도록 구조화합니다. 

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
이 때 출력된 해당 절대 경로를 `<PATH>` 라 할 때, `AIRFLOW HOME` 디렉토리의 `airflow.cfg` 파일의 `dags_folder` 값이 다음과 같이 출력되도록 수정합니다.
```bash
$ cat airflow.cfg | grep dags_folder
dags_folder = <PATH>/dags
```
이후, `airflow standalone` 으로 에어플로우 서버를 재시작할 시 DAG들은 <PATH>/dags 레포지토리에서부터 읽어지게 됩니다.

## Variable Setting
본 DAG 들은 에어플로우 서버에 저장된 환경변수를 통해 데이터를 저장하고 불러올 위치를 지정하고 있습니다. 

이 패키지에서 요구하는 변수는 다음과 같습니다.
- `TP_PATH`: 각 DAGs에서 처리한 데이터를 저장하는 위치의 절대경로입니다. 해당 디렉토리가 없을 경우 새로이 형성되며, `/home/<USER>/<YOUR_PATH>` 같은 식으로 설정해주시면 됩니다. 각 DAGs들은 해당 경로에 하위폴더를 생성해 각각 데이터를 저장하게 됩니다.

이 변수는 에어플로우 웹서버에서 관리자 권한을 통해 중앙 메뉴창의  `Admin` - `Variables`를 통해 들어가 생성 및 변경할 수 있으며, 

![image](https://github.com/user-attachments/assets/f7007910-9d6a-4670-bc5d-7e692586e8bc)

혹은 다음과 같은 셸 커맨드로도  설정할 수 있습니다. 

```bash
$ airflow variables set TP_PATH "/home/<USER>/<YOUR_PATH>"

# airflow message...
Variable TP_PATH created
```


## DAGs 별 기능
- `extract1.py`, `extract2.py`, `extract3.py`

 : 영화 박스오피스 데이터의 수집을 위한 DAG 파일들로, 공통적으로 `extract_package` 패키지를 사용합니다. 각각의 파일은 2023년을 3개 분기로 나누어 1 - 4월, 5 - 8월, 9 - 12월의 데이터를 각각 수집해 지정된 경로에 일차적으로 저장합니다. 

- `transform1.py`, `transform2.py`, `transform3.py`

 : 직전 3개 DAG에서 수집한 데이터들을 가공해 다시 저장하는 DAG들로, 공통적으로 `transform_package` 패키지를 사용합니다. 데이터들은 `extract` DAG들이 각각  나누어 저장한 것을 그대로 이용합니다. 

- `load1.py`, `load2.py`, `load3.py`

: `transform` DAG에서 처리한 데이터를 행을 구별해 `parquet` 형식으로 최종적으로 저장하는 DAG입니다. 공통적으로 `load_package` 패키지를 사용합니다.

- `ice_breaking()`: DAGs가 작동하는지 확인하기 위해 넣은 디버깅 겸 이스터에그 함수입니다. 모든 DAGs는 최종적으로 종료하기 전에 본 함수를 불러 프로젝트 멤버들의 정면사진을 ASCII 아트 방식으로 로그에 출력합니다. 해당 함수는 `extract_package` 패키지에 속해 있습니다.
