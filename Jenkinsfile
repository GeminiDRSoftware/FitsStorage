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

    options { skipDefaultCheckout() }

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

//                 checkout scm

                echo 'Checking Out FitsStorage'
                dir('FitsStorage') {
                    git url: 'git@gitlab.gemini.edu:DRSoftware/FitsStorage.git',
                    branch: '2021-2',
                    credentialsId: '23171fd7-22a8-459a-bbf3-ec2e65ec56b7'
                }

                echo 'Checking Out FitsStorageConfig'
                dir('FitsStorageConfig') {
                    git url: 'git@gitlab.gemini.edu:DRSoftware/FitsStorageConfig.git',
                    branch: '2021-2',
                    credentialsId: '23171fd7-22a8-459a-bbf3-ec2e65ec56b7'
                }

                echo 'Checking Out FitsStorageDB'
                dir('FitsStorageDB') {
                    git url: 'git@gitlab.gemini.edu:DRSoftware/FitsStorageDB.git',
                    branch: 'release/1.0.x',
                    credentialsId: '23171fd7-22a8-459a-bbf3-ec2e65ec56b7'
                }

                echo 'Checking Out GeminiCalMgr'
                dir('GeminiCalMgr') {
                    git url: 'git@gitlab.gemini.edu:DRSoftware/GeminiCalMgr.git',
                    branch: 'release/1.1.x',
                    credentialsId: '23171fd7-22a8-459a-bbf3-ec2e65ec56b7'
                }
            }

        }

        stage('Building Docker Containers') {
            steps {
                script {
                    def utilsimage = docker.build("gemini/fitsarchiveutils:jenkins", " -f FitsStorage/docker/fitsstorage-jenkins/Dockerfile .")
                    def archiveimage = docker.build("gemini/archive:jenkins", " -f FitsStorage/docker/archive-jenkins/Dockerfile .")
                    sh '''
                    docker network create fitsstorage-jenkins || true
                    docker container rm fitsdata-jenkins || true
                    docker container rm archive-jenkins || true
                    '''
                    def postgres = docker.image('postgres:12').withRun(" --network fitsstorage-jenkins --name fitsdata-jenkins -e POSTGRES_USER=fitsdata -e POSTGRES_PASSWORD=fitsdata -e POSTGRES_DB=fitsdata") { c ->
                        def archive = docker.image("gemini/archive:jenkins").withRun(" --network fitsstorage-jenkins --name archive-jenkins -e FITS_DB_SERVER=\"fitsdata:fitsdata@fitsdata-jenkins\" -e TEST_IMAGE_PATH=/tmp/archive_test_images -e TEST_IMAGE_CACHE=/tmp/cached_archive_test_images -e CREATE_TEST_DB=False -e BLOCKED_URLS=\"\" -e PYTHONPATH=/opt/FitsStorage:/opt/DRAGONS:/opt/FitsStorageDB:/opt/GeminiCalMgr") { a->
                            try {
                                docker.image('gemini/fitsarchiveutils:jenkins').inside(" -v /data/pytest_tmp:/tmp  --network fitsstorage-jenkins -e STORAGE_ROOT=/tmp/jenkins_pytest/dataflow -e FITS_DB_SERVER=\"fitsdata:fitsdata@fitsdata-jenkins\" -e PYTEST_SERVER=http://archive-jenkins -e TEST_IMAGE_PATH=/tmp/archive_test_images -e TEST_IMAGE_CACHE=/tmp/cached_archive_test_images -e BLOCKED_URLS=\"\" -e CREATE_TEST_DB=False -e PYTHONPATH=/opt/FitsStorage:/opt/DRAGONS:/opt/FitsStorageDB:/opt/GeminiCalMgr") {
                                    sh 'python3 /opt/FitsStorage/fits_storage/scripts/create_tables.py'
                                    echo "Running tests against docker containers"
                                    sh  '''
                                        cd /opt/FitsStorage
                                        mkdir -p /tmp/archive_test_images
                                        mkdir -p /tmp/cached_archive_test_images
                                        env PYTEST_SERVER=http://archive-jenkins coverage run --omit "/usr/lib/*,/usr/local/*,/opt/DRAGONS/*,tests/*" -m pytest tests
                                        coverage report -m --fail-under=63
                                        '''
                                }
                            } catch (exc) {
                                sh "docker logs ${a.id}"
                                sh "docker logs archive-jenkins"
                                throw exc
                            }
                        }
                    }
                }
            }
        }
        stage('Deploy To Host') {
            when {
                expression { deploy_target != 'none' }
            }
            steps {
                dir('FitsStorage') {
                    echo "Deploying to ${deploy_target} Host"
                    ansiblePlaybook(
                        inventory: 'ansible/${deploy_target}',
                        playbook: 'ansible/playbooks/archive_install.yml',
                        disableHostKeyChecking: true,
                        credentialsId: '23171fd7-22a8-459a-bbf3-ec2e65ec56b7',
                        vaultCredentialsId: 'vault_pass',
                        extras: " --extra-vars '@/var/lib/jenkins/secret.yml'"
                    )
                }
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
