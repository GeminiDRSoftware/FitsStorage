#!/usr/bin/env groovy
/*
 * Jenkins Pipeline for DRAGONS
 *
 * by Bruno C. Quint
 * adapted for FitsStorage by Oliver Oberdorf
 *
 * Required Plug-ins:
 * - CloudBees File Leak Detector
 * - Cobertura Plug-in
 * - Warnings NG
 */

pipeline {
    agent any

    environment {
        PATH = "$JENKINS_HOME/anaconda3-dev-oly/bin:$PATH"
        CONDA_ENV_FILE = ".jenkins/conda_py3env_stable.yml"
        CONDA_ENV_NAME_DEPRECATED = "py3_stable"
        CONDA_ENV_NAME = "fitsstorage_pipeline_venv"
        TEST_IMAGE_PATH = "/tmp/archive_test_images"
        TEST_IMAGE_CACHE = "/tmp/cached_archive_test_images"
    }

    stages {
        stage ("Prepare"){

            steps{
                echo 'STARTED'
                checkout scm
                sh 'mkdir -p ${TEST_IMAGE_PATH}'
                sh 'mkdir -p ${TEST_IMAGE_CACHE}'
                sh 'rm -rf ./plots; mkdir -p ./plots'
                sh 'rm -rf ./reports; mkdir -p ./reports'
                sh '.jenkins/scripts/download_and_install_anaconda.sh'
                sh '.jenkins/scripts/create_conda_environment.sh'
                sh '''. activate ${CONDA_ENV_NAME}
                      '''
                sh 'conda list -n ${CONDA_ENV_NAME}'
                sh '''
                    echo /tmp/DRAGONS-$$ > dragons-repo.txt
                    git clone https://github.com/GeminiDRSoftware/DRAGONS.git `cat dragons-repo.txt`
                '''
            }

        }

        stage('Building Docker Containers') {
            steps {
                script {
                    def utilsimage = docker.build("gemini/fitsarchiveutils:jenkins", " -f docker/fitsstorage-jenkins/Dockerfile .")
                    def archiveimage = docker.build("gemini/archive:jenkins", " -f docker/archive-jenkins/Dockerfile .")
                    sh '''
                    docker network create fitsstorage-jenkins || true
                    docker container rm fitsdata-jenkins || true
                    docker container rm archive-jenkins || true
                    '''
                    def postgres = docker.image('postgres:12').withRun(" --network fitsstorage-jenkins --name fitsdata-jenkins -e POSTGRES_USER=fitsdata -e POSTGRES_PASSWORD=fitsdata -e POSTGRES_DB=fitsdata") { c ->
                        def archive = docker.image("gemini/archive:jenkins").withRun(" --network fitsstorage-jenkins --name archive-jenkins -e FITS_DB_SERVER=\"fitsdata:fitsdata@fitsdata-jenkins\" -e TEST_IMAGE_PATH=/tmp/archive_test_images -e TEST_IMAGE_CACHE=/tmp/cached_archive_test_images -e CREATE_TEST_DB=False -e PYTHONPATH=/opt/FitsStorage:/opt/DRAGONS") { a->
                            docker.image('gemini/fitsarchiveutils:jenkins').inside("  --network fitsstorage-jenkins -e FITS_DB_SERVER=\"fitsdata:fitsdata@fitsdata-jenkins\" -e PYTEST_SERVER=http://archive-jenkins -e TEST_IMAGE_PATH=/tmp/archive_test_images -e TEST_IMAGE_CACHE=/tmp/cached_archive_test_images -e CREATE_TEST_DB=False -e PYTHONPATH=/opt/FitsStorage:/opt/DRAGONS") {
                                sh 'python3 fits_storage/scripts/create_tables.py'
                                echo "Running tests against docker containers"
                                sh  '''
                                    mkdir -p /tmp/archive_test_images
                                    mkdir -p /tmp/cached_archive_test_images
                                    pytest tests
                                    '''
                            }
                        }
                    }
                }
            }
        }

//         stage('Unit tests') {
//
//             steps {
//                 echo "Activating Conda environment"
//                 sh '''. activate ${CONDA_ENV_NAME}
//                       '''
//                 echo "ensure cleaning __pycache__"
//                 sh  'find . | grep -E "(__pycache__|\\.pyc|\\.pyo$)" | xargs rm -rfv'
//
//                 echo "Running tests"
//                 sh  '''
//                     export CREATE_TEST_DB=False
//                     . activate ${CONDA_ENV_NAME}
//                     export PYTHONPATH=`cat dragons-repo.txt`
//                     echo running python `which python`
//                     echo running pytest `which pytest`
//                     echo running coverage `which coverage`
//                     export VALIDATION_DEF_PATH=./docs/dataDefinition/
//                     #coverage run -m pytest --junit-xml ./reports/unittests_results.xml tests
//                     '''
//             }
//
//         }

    }
    post {
        always {
          junit (
            allowEmptyResults: true,
            testResults: 'reports/*_results.xml'
            )
          sh '''
             if [ -f dragons-repo.txt ]; then rm -rf `cat dragons-repo.txt`; fi
             docker rmi gemini/fitsarchiveutils:jenkins || true
             docker rmi gemini/archive:jenkins || true
             docker network rm fitsstorage-jenkins || true
          '''
        }
        success {
            echo 'SUCCESSFUL'
        }
        failure {
            echo 'FAILED'
        }
    }
}
