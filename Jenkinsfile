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

    agent {
        label 'bquint-ld1'
    }

    triggers {
        pollSCM('H * * * *')  // Polls Source Code Manager every hour
    }

    options {
        skipDefaultCheckout(true)
        buildDiscarder(logRotator(numToKeepStr: '10'))
        timestamps()
    }

    environment {
        PATH = "$JENKINS_HOME/anaconda3-dev-oly/bin:$PATH"
        CONDA_ENV_FILE = ".jenkins/conda_py3env_stable.yml"
        CONDA_ENV_NAME_DEPRECATED = "py3_stable"
        CONDA_ENV_NAME = "fitsstorage_pipeline_venv"
    }

    stages {

        stage ("Prepare"){

            steps{
                sendNotifications 'STARTED'
                checkout scm
                sh 'rm -rf ./plots; mkdir -p ./plots'
                sh 'rm -rf ./reports; mkdir -p ./reports'
                sh '.jenkins/scripts/download_and_install_anaconda.sh'
                sh '.jenkins/scripts/create_conda_environment.sh'
                sh '''source activate ${CONDA_ENV_NAME}
                      '''
                sh 'conda list -n ${CONDA_ENV_NAME}'
            }

        }

        stage('Unit tests') {

            steps {

                echo "ensure cleaning __pycache__"
                sh  'find . | grep -E "(__pycache__|\\.pyc|\\.pyo$)" | xargs rm -rfv'

                echo "Running tests"
                sh  '''
                    source activate ${CONDA_ENV_NAME}
                    coverage run -m pytest -m "not integtest and not gmosls" --junit-xml ./reports/unittests_results.xml
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
        }
        success {
            sendNotifications 'SUCCESSFUL'
        }
        failure {
            sendNotifications 'FAILED'
        }
    }
}
