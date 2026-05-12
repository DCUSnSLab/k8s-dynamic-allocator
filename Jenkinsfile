def selectedImages = []

def allImages = {
    return ['controller', 'compute_pod', 'user_pod', 'swlabssh']
}

def changedAny = { List files, List paths ->
    return files.any { file ->
        paths.any { path ->
            file == path || file.startsWith(path)
        }
    }
}

def imageEnabled = { String imageName ->
    return selectedImages.contains(imageName)
}

def dockerBuildAndPush = { String imageName, String dockerfile, String contextPath ->
    def image = "${env.HARBOR_PROJECT}/${imageName}"
    sh """
        docker build \
          -f ${dockerfile} \
          -t ${image}:${env.IMAGE_TAG} \
          -t ${image}:latest \
          ${contextPath}

        docker push ${image}:${env.IMAGE_TAG}
        docker push ${image}:latest
    """
}

pipeline {
    agent any

    options {
        disableConcurrentBuilds()
        skipDefaultCheckout(true)
    }

    parameters {
        choice(
            name: 'TARGET_IMAGE',
            choices: ['auto', 'all', 'controller', 'compute_pod', 'user_pod', 'swlabssh'],
            description: 'Image target to build. auto builds only images affected by the latest Git changes.'
        )
    }

    environment {
        HARBOR_REGISTRY = 'harbor.cu.ac.kr'
        HARBOR_PROJECT = 'harbor.cu.ac.kr/k8s_dynamic_allocator'
        HARBOR_CREDENTIALS_ID = 'harbor'
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
                    env.IMAGE_TAG = "${env.GIT_SHA7}-${env.BUILD_NUMBER}"

                    if (params.TARGET_IMAGE == 'all') {
                        selectedImages = allImages()
                    } else if (params.TARGET_IMAGE != 'auto') {
                        selectedImages = [params.TARGET_IMAGE]
                    } else {
                        def diffBase = env.GIT_PREVIOUS_SUCCESSFUL_COMMIT ?: env.GIT_PREVIOUS_COMMIT ?: ''

                        if (!diffBase?.trim()) {
                            def hasParent = sh(
                                script: 'git rev-parse --verify HEAD~1 >/dev/null 2>&1',
                                returnStatus: true
                            ) == 0
                            if (hasParent) {
                                diffBase = sh(
                                    script: 'git rev-parse HEAD~1',
                                    returnStdout: true
                                ).trim()
                            }
                        }

                        if (!diffBase?.trim()) {
                            selectedImages = allImages()
                        } else {
                            def diffOutput = sh(
                                script: "git diff --name-only ${diffBase} HEAD",
                                returnStdout: true
                            ).trim()
                            def changedFiles = diffOutput ? diffOutput.split('\\n') as List : []

                            if (changedFiles.contains('Jenkinsfile')) {
                                selectedImages = allImages()
                            } else {
                                selectedImages = []

                                if (changedAny(changedFiles, [
                                    'controller/',
                                    'deploy/docker/controller/',
                                    'deploy/controller.yaml',
                                    'kda_config.py'
                                ])) {
                                    selectedImages.add('controller')
                                }

                                if (changedAny(changedFiles, [
                                    'compute_agent/',
                                    'deploy/docker/compute/',
                                    'controller/manifests/compute-general.yaml',
                                    'kda_config.py'
                                ])) {
                                    selectedImages.add('compute_pod')
                                }

                                if (changedAny(changedFiles, [
                                    'dcusshk8s/dockerbuild/'
                                ])) {
                                    selectedImages.add('user_pod')
                                }

                                if (changedFiles.any { file ->
                                    (file.startsWith('dcusshk8s/') && !file.startsWith('dcusshk8s/dockerbuild/')) ||
                                    file.startsWith('deploy/docker/swlabssh/') ||
                                    file == 'deploy/swlabssh.yaml' ||
                                    file == 'kda_config.py'
                                }) {
                                    selectedImages.add('swlabssh')
                                }
                            }
                        }
                    }

                    echo "IMAGE_TAG=${env.IMAGE_TAG}"
                    echo "TARGET_IMAGE=${params.TARGET_IMAGE}"
                    echo "SELECTED_IMAGES=${selectedImages.join(',') ?: 'none'}"
                }
            }
        }

        stage('Build and Push controller') {
            when {
                expression { imageEnabled('controller') }
            }
            steps {
                script {
                    docker.withRegistry("https://${env.HARBOR_REGISTRY}", env.HARBOR_CREDENTIALS_ID) {
                        dockerBuildAndPush(
                            'controller',
                            'deploy/docker/controller/Dockerfile',
                            '.'
                        )
                    }
                }
            }
        }

        stage('Build and Push compute_pod') {
            when {
                expression { imageEnabled('compute_pod') }
            }
            steps {
                script {
                    docker.withRegistry("https://${env.HARBOR_REGISTRY}", env.HARBOR_CREDENTIALS_ID) {
                        dockerBuildAndPush(
                            'compute_pod',
                            'deploy/docker/compute/Dockerfile',
                            '.'
                        )
                    }
                }
            }
        }

        stage('Build and Push user_pod') {
            when {
                expression { imageEnabled('user_pod') }
            }
            steps {
                dir('dcusshk8s/dockerbuild') {
                    script {
                        docker.withRegistry("https://${env.HARBOR_REGISTRY}", env.HARBOR_CREDENTIALS_ID) {
                            dockerBuildAndPush(
                                'user_pod',
                                'Dockerfile',
                                '.'
                            )
                        }
                    }
                }
            }
        }

        stage('Build and Push swlabssh') {
            when {
                expression { imageEnabled('swlabssh') }
            }
            steps {
                script {
                    docker.withRegistry("https://${env.HARBOR_REGISTRY}", env.HARBOR_CREDENTIALS_ID) {
                        dockerBuildAndPush(
                            'swlabssh',
                            'deploy/docker/swlabssh/Dockerfile',
                            '.'
                        )
                    }
                }
            }
        }

    }

    post {
        success {
            echo "Build completed with IMAGE_TAG=${env.IMAGE_TAG}"
        }
    }
}
