// The Docker Pipeline plugin's build/push API, matching the AIBootcamp and
// smartfarmHMI pipelines. The second docker.build argument is appended to
// `docker build`, so the build context has to come last.
def dockerBuild = { String imageName, String dockerfile, String contextPath, String buildArgs ->
    docker.build(
        "${env.HARBOR_PROJECT}/${imageName}:${env.IMAGE_TAG}",
        "${buildArgs} -f ${dockerfile} ${contextPath}"
    )
}

// Resolved by name rather than carrying the Image object across stages, so the
// build, verify, and push stages stay independent. Must run inside
// docker.withRegistry to be authenticated.
def dockerPush = { String imageName ->
    def image = docker.image("${env.HARBOR_PROJECT}/${imageName}:${env.IMAGE_TAG}")
    image.push()
    image.push('latest')
}

pipeline {
    agent any

    options {
        timeout(time: 60, unit: 'MINUTES')
        disableConcurrentBuilds()
        skipDefaultCheckout(true)
    }

    // Multibranch note: these appear in the UI only from the second build of a
    // branch. The first indexing build sees params as null, so every read below
    // falls back to the default.
    parameters {
        string(
            name: 'POOL_AVAILABLE_MIN',
            defaultValue: '2',
            description: 'R - warm Compute Pods kept immediately allocatable'
        )
        string(
            name: 'POOL_TOTAL_MAX',
            defaultValue: '5',
            description: 'N - upper bound on available + assigned Compute Pods'
        )
    }

    environment {
        HARBOR_REGISTRY = 'harbor.cu.ac.kr'
        HARBOR_PROJECT = 'harbor.cu.ac.kr/k8s_dynamic_allocator'
        HARBOR_CREDENTIALS_ID = 'harbor'

        DEPLOY_NAMESPACE = 'kda-test'
        DEPLOY_STORAGE_CLASS = 'normal-r3'
        DEPLOY_OVERLAY = 'deploy/overlays/dev'
        DEPLOY_LOCK = 'kda-deploy-dev'
        DEPLOY_STAGE_LABEL = 'k8s-dynamic-allocator/deploy-stage'

        DEPLOY_SSH_HOST = '203.250.35.87'
        DEPLOY_SSH_PORT = '30622'
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
                sh 'git submodule update --init --recursive'
            }
        }

        stage('Prepare') {
            steps {
                script {
                    env.GIT_SHA7 = sh(
                        script: 'git rev-parse --short=7 HEAD',
                        returnStdout: true
                    ).trim()
                    env.IMAGE_TAG = "${env.BUILD_NUMBER}-${env.GIT_SHA7}"

                    env.CONTROLLER_IMAGE = "${env.HARBOR_PROJECT}/controller:${env.IMAGE_TAG}"
                    env.COMPUTE_POD_IMAGE = "${env.HARBOR_PROJECT}/compute_pod:${env.IMAGE_TAG}"
                    env.USER_POD_IMAGE = "${env.HARBOR_PROJECT}/user_pod:${env.IMAGE_TAG}"
                    env.SWLABSSH_IMAGE = "${env.HARBOR_PROJECT}/swlabssh:${env.IMAGE_TAG}"

                    def userCauses = currentBuild.getBuildCauses(
                        'hudson.model.Cause$UserIdCause'
                    )
                    env.IS_MANUAL_BUILD = userCauses.isEmpty() ? 'false' : 'true'
                    env.DEPLOY_STARTED = 'false'
                    env.DEPLOY_DIAGNOSTICS_DONE = 'false'

                    env.POOL_AVAILABLE_MIN = (params.POOL_AVAILABLE_MIN ?: '2').trim()
                    env.POOL_TOTAL_MAX = (params.POOL_TOTAL_MAX ?: '5').trim()

                    echo "BRANCH_NAME=${env.BRANCH_NAME}"
                    echo "IMAGE_TAG=${env.IMAGE_TAG}"
                    echo "IS_MANUAL_BUILD=${env.IS_MANUAL_BUILD}"
                    echo "POOL_POLICY R=${env.POOL_AVAILABLE_MIN} N=${env.POOL_TOTAL_MAX}"

                    if (env.IS_MANUAL_BUILD != 'true') {
                        echo 'Automatic Multibranch/SCM build detected: image build and deployment are skipped.'
                    }
                }
            }
        }

        stage('Build Base Images') {
            when {
                expression { env.IS_MANUAL_BUILD == 'true' }
            }
            parallel {
                stage('compute_pod') {
                    steps {
                        script {
                            dockerBuild(
                                'compute_pod',
                                'deploy/docker/compute/Dockerfile',
                                '.',
                                ''
                            )
                        }
                    }
                }

                stage('user_pod') {
                    steps {
                        dir('dcusshk8s/dockerbuild') {
                            script {
                                dockerBuild(
                                    'user_pod',
                                    'Dockerfile',
                                    '.',
                                    ''
                                )
                            }
                        }
                    }
                }
            }
        }

        stage('Build Dependent Images') {
            when {
                expression { env.IS_MANUAL_BUILD == 'true' }
            }
            parallel {
                stage('controller') {
                    steps {
                        script {
                            dockerBuild(
                                'controller',
                                'deploy/docker/controller/Dockerfile',
                                '.',
                                "--build-arg COMPUTE_POD_IMAGE=${env.COMPUTE_POD_IMAGE}"
                            )
                        }
                    }
                }

                stage('swlabssh') {
                    steps {
                        script {
                            dockerBuild(
                                'swlabssh',
                                'deploy/docker/swlabssh/Dockerfile',
                                '.',
                                "--build-arg USER_POD_IMAGE=${env.USER_POD_IMAGE}"
                            )
                        }
                    }
                }
            }
        }

        stage('Verify Built Images') {
            when {
                expression { env.IS_MANUAL_BUILD == 'true' }
            }
            steps {
                sh 'sh deploy/scripts/verify_images.sh'
            }
        }

        stage('Push Images') {
            when {
                expression { env.IS_MANUAL_BUILD == 'true' }
            }
            steps {
                script {
                    docker.withRegistry(
                        "https://${env.HARBOR_REGISTRY}",
                        env.HARBOR_CREDENTIALS_ID
                    ) {
                        ['compute_pod', 'user_pod', 'controller', 'swlabssh'].each {
                            dockerPush(it)
                        }
                    }
                }
            }
        }

        stage('Deploy') {
            when {
                expression { env.IS_MANUAL_BUILD == 'true' }
            }
            steps {
                script {
                    lock(resource: env.DEPLOY_LOCK) {
                        env.DEPLOY_STARTED = 'true'
                        try {
                            sh 'sh deploy/scripts/deploy.sh'
                        } catch (deploymentError) {
                            sh 'sh deploy/scripts/debug.sh'
                            env.DEPLOY_DIAGNOSTICS_DONE = 'true'
                            throw deploymentError
                        }
                    }
                }
            }
        }
    }

    post {
        success {
            script {
                if (env.IS_MANUAL_BUILD == 'true') {
                    echo "Deployment completed with IMAGE_TAG=${env.IMAGE_TAG}"
                } else {
                    echo 'Automatic Multibranch/SCM build completed without image build or deployment.'
                }
            }
        }

        failure {
            script {
                if (
                    env.IS_MANUAL_BUILD == 'true' &&
                    env.DEPLOY_STARTED == 'true' &&
                    env.DEPLOY_DIAGNOSTICS_DONE != 'true'
                ) {
                    sh 'sh deploy/scripts/debug.sh'
                }
            }
        }
    }
}
