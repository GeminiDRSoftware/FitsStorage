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
                sh '''source activate ${CONDA_ENV_NAME}
                      '''
                sh 'conda list -n ${CONDA_ENV_NAME}'
                sh '''
                    echo /tmp/DRAGONS-$$ > dragons-repo.txt
                    git clone https://github.com/GeminiDRSoftware/DRAGONS.git `cat dragons-repo.txt`
                '''
                sh '''
                    export DOCKER=""
                    if [ -f /usr/local/bin/docker ]; then
                        export DOCKER=/usr/local/bin/docker
                    fi
                    if [ -f /usr/bin/docker ]; then
                        export DOCKER=/usr/bin/docker
                    fi
                    if [ -f /bin/docker ]; then
                        export DOCKER=/bin/docker
                    fi
                    if [ -f /usr/local/sbin/docker ]; then
                        export DOCKER=/usr/local/sbin/docker
                    fi
                    if [ -f /usr/sbin/docker ]; then
                        export DOCKER=/usr/sbin/docker
                    fi
                    if [ -f /sbin/docker ]; then
                        export DOCKER=/sbin/docker
                    fi
                    if [ -f /opt/docker/bin/docker ]; then
                        export DOCKER=/opt/docker/bin/docker
                    fi
                '''
            }

        }

        stage('Unit tests') {

            steps {
                echo "Activating Conda environment"
                sh '''source activate ${CONDA_ENV_NAME}
                      '''
                echo "ensure cleaning __pycache__"
                sh  'find . | grep -E "(__pycache__|\\.pyc|\\.pyo$)" | xargs rm -rfv'

                echo "Checking Docker configuration"
                sh '''
                    echo Docker location: $DOCKER
                    if [ "" = "$DOCKER" ]; then
                        echo No Docker found
                    fi
                '''

                echo "Running tests"
                sh  '''
                    source activate ${CONDA_ENV_NAME}
                    export PYTHONPATH=`cat dragons-repo.txt`
                    echo running python `which python`
                    echo running pytest `which pytest`
                    echo running coverage `which coverage`
                    coverage run -m pytest --junit-xml ./reports/unittests_results.xml tests
                    '''
            }

        }

    }
    post {
        always {
          junit (
            allowEmptyResults: true,
            testResults: 'reports/*_results.xml'
            )
          sh '''
             if [ -f dragons-repo.txt ]; then rm -rf `cat dragons-repo.txt`; fi
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
