# Terrific Totes ETL Pipeline

<details>
<summary>Table of Contents</summary>

1. [About The Project](#about-the-project) 
    - [Built With](#built-with)
2. [Getting Started](#getting-started)  
    - [Prerequisites](#prerequisites)
    - [Installation](#installation)
3. [Contributors](#contributors)
4. [License](#license)
5. [Contact](#contact)
6. [Acknowledgements](#acknowledgements)

</details>

## About The Project
This is an end-to-end ETL pipeline for a tote bag business.

It pulls data from their database into a data warehouse for future analysis. 

In this projet, three lambda applications were created using and psycopg2 and boto3. They were deployed in the AWS cloud storage services s3 using terraform and github actions CI/CD. 

![image](https://github.com/user-attachments/assets/449de78f-f422-4bc3-8dd5-023e70faabc3)

**Lambda 1**

This lambda function handles the extraction. It connects to the database using psycopg2, runs on a schedule, monitors for changes, and pushes data into an s3 bucket in csv format.

It logs to cloudwatch and sends out failure alerts via email.

**Lambda 2**

The transformation step cleans and reshapes the data into predefined schemas for warehousing. It uses pandas and boto3 and stores the cleaned output as parquet files into a separate s3 bucket.

It logs to cloudwatch and sends out failure alerts via email.

**Lambda 3**

The load lambda takes the processed data from the last s3 bucket and loads it into the data warehouse for future analysis.

It logs to cloudwatch and sends out failure alerts via email.


### Built With
- Python
- Terraform
- PostgreSQL
- SQLAlchemy

## Getting Started
To get a local copy of this pipeline up and running, make sure all the prerequisites are met.

### Prerequisites
**These instructions assume that you already have python3 installed locally. If you do not, follow [this link](https://www.python.org/downloads/) to do so.**

- **terraform**

     Follow [this link](https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli) to get terraform installed locally.

- **postgresql**

    Follow [this link](https://www.postgresql.org/download/) to get postgres installed locally.

- **aws**

    Follow [this link](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) to get aws installed locally.

### Installation
1. Fork and clone the repo.
```
git clone https://github.com/ETL-Totally-Totes/ETL-totally-totes.git
```

2. Configure AWS locally
   - On the AWS console, create a new IAM user for this pipeline, or use an existing one if you would like.
   - If you are creating a new user, follow the Principle of Least Privilege. Only give permissions for the resources you see in the project's terraform directory.
   - Once you have created the user, copy or save the access key and secret access key.
   - Run the command below and fill out any details it asks.
  ```
  aws configure
  ```

3. Install requirements and other dependencies
```
make run-all
```
3. If you are making any changes to the code or testing it, add the following to your .env file.
```
PG_USERNAME=**** # OLTP username
PG_DATABASE=**** # OLTP database name
PG_PASSWORD=**** # OLTP password
PG_HOST=**** # OLTP host

TEST_PG_DATABASE=test-_totes
TEST_PG_PASSWORD=**** # Your local psql password
TEST_PG_USERNAME=**** # Your local psql username
TEST_PG_PORT=5432

BUCKET=****  #Ingestion bucket name 
TRANSFORM_BUCKET=*** # Transformation bucket name
```
4. In maintain security, set up the following secrets in the aws secrets manager .
  - database_secret:
      Needs to contain details to the OLTP
      - username
      - database
      - password
      - host
  - warehouse_secret:
      Needs to contain:
      - PG_CONNECTION:
        
        The format for this is below:
      ```
      {database_type}://{username}:{password}@{host}:{port}/{database_name}
      
      e.g. for postgres:
      
      postgresql://username:password@localhost:5432/your_database
      ```
5. In the vars.tf, change all variable except the python runtime to your own. 

  Due to versioning issues with the psycopg2 module found during testing, we recommend python 3.12 for the transform lambda.
  
  If you make changes to the code and require more dependencies for lambdas that use the AWS Pandas SDK, make note of this.
  To prevent the need for containers and to maintain overall layer size under 50mb, we recommend changing the runtime to python3.12 especially if your layer is exceeding 50mb.
  Ensure that in this situation, the AWS Pandas SDK matches this version.

7. Set up an s3 bucket for the backend tfstate storage. You can do so quickly in the console.
```
aws s3 mb s3://{bucket_name}
```
Afterwards update this name in the main.tf

8. Though the dependencies zip files have been provided with the repo, it is recommended to create your own again, especially if you require more dependencies.

If you add more dependencies, update them in the make_zip.sh for the respective lambda which needs it. Note that the requirements_dev.txt only contains dependencies that aws natively supports and provides. Update any extra dependencies you might need in the main requirements.txt

In the repo root, run:
```
chmod +x make_zip.sh

./make_zip.sh
```

10. To set up the infrastructure for the pipeline, run:
```
terraform init

terraform plan

terraform apply 

```

## Contributors
Any contributions you make are greatly appreciated.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement". Don't forget to give the project a star! Thanks again!

<table border="0" cellspacing="0" cellpadding="0">
  <tr>
    <td align="center">
      <a href="https://github.com/AbbyAdj">
        <img src="https://avatars.githubusercontent.com/AbbyAdj" width="60" height="60" alt="AbbyAdj"/>
        <br />
        <sub><b>AbbyAdj</b></sub>
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/saffi444">
        <img src="https://avatars.githubusercontent.com/saffi444" width="60" height="60" alt="saffi444"/>
        <br />
        <sub><b>saffi444</b></sub>
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/Dalebar">
        <img src="https://avatars.githubusercontent.com/Dalebar" width="60" height="60" alt="Dalebar"/>
        <br />
        <sub><b>Dalebar</b></sub>
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/georgilar">
        <img src="https://avatars.githubusercontent.com/georgilar" width="60" height="60" alt="georgilar"/>
        <br />
        <sub><b>georgilar</b></sub>
      </a>
    </td>
    <td align="center">
      <a href="https://github.com/SelvitaPR">
        <img src="https://avatars.githubusercontent.com/SelvitaPR" width="60" height="60" alt="SelvitaPR"/>
        <br />
        <sub><b>SelvitaPR</b></sub>
      </a>
    </td>
  </tr>
</table>

## License
This project is licensed under the MIT License.

## Contact

Contact any of the following project developers if you have any questions or would like to collaborate:

Abigail Adjei - [abby.adjei@example.com](mailto:abby.adjei@example.com) 

Saffron Morton - [smorton@gmx.co.uk](mailto:smorton@gmx.co.uk)

Dale Barnes - [dalewithvan@gmail.com](mailto:dalewithvan@gmail.com)

George Krokos - [george.krokos1@gmail.com](mailto:george.krokos1@gmail.com)

Patricia Selva Plaza Rojas - [selva.pla.roj@gmail.com](mailto:selva.pla.roj@gmail.com)


## Acknowledgements
We are grateful to [Northcoders](https://www.northcoders.com/) for their constant guidance and support throughout the bootcamp and during the development of this project.

