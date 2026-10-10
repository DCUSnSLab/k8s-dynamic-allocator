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
            name: 'BUFFER_RESERVE',
            defaultValue: '2',
            description: 'R - warm Compute Pods kept immediately allocatable'
        )
        string(
            name: 'BUFFER_CAPACITY',
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

                    env.BUFFER_RESERVE = (params.BUFFER_RESERVE ?: '2').trim()
                    env.BUFFER_CAPACITY = (params.BUFFER_CAPACITY ?: '5').trim()

                    echo "BRANCH_NAME=${env.BRANCH_NAME}"
                    echo "IMAGE_TAG=${env.IMAGE_TAG}"
                    echo "IS_MANUAL_BUILD=${env.IS_MANUAL_BUILD}"
                    echo "BUFFER_POLICY R=${env.BUFFER_RESERVE} N=${env.BUFFER_CAPACITY}"

                    if (env.IS_MANUAL_BUILD != 'true') {
                        echo 'Automatic Multibranch/SCM build detected: image build and deployment are skipped.'
                    }
                }
            }
        }

        // 이미지 빌드보다 앞이고, 수동 빌드 조건을 걸지 않는다. 870d536 은 포화에서
        // 목표 replicas 를 3 에 고정시켜 처리량을 39% 깎았는데, 그 회귀가 배포까지
        // 간 이유가 "테스트를 돌리는 단계가 파이프라인에 없었다" 는 것이다. 자동
        // SCM 빌드에서도 걸려야 그 역할을 한다.
        //
        // 에이전트에 python 이 깔려 있다고 가정하지 않는다. 테스트는 importlib 로
        // 대상 모듈만 직접 읽어 Redis/HTTP 런타임 없이 돌므로 의존성 설치가 없다.
        stage('Unit Tests') {
            steps {
                script {
                    // docker.image().inside() 는 이 에이전트에서 쓸 수 없다. 호스트
                    // 도커 데몬 + emptyDir 워크스페이스 조합이라 바인드 마운트가 빈
                    // 디렉터리가 되고 "process apparently never started" 로 죽는다
                    // (빌드 48). 테스트는 Dockerfile 의 RUN 으로 돌려 실패를 빌드
                    // 실패로 전파한다 - 자세한 이유는 그 파일 주석에 있다.
                    def tests = docker.build(
                        "kda-unit-tests:${env.IMAGE_TAG}",
                        '-f deploy/docker/tests/Dockerfile .'
                    )
                    // 에이전트에 이미지가 쌓이지 않게 지운다. 실패하면 애초에
                    // 만들어지지 않으므로 이 줄은 성공 경로에만 온다.
                    sh "docker image rm -f ${tests.id} || true"
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
