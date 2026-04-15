pipeline {
    agent any

    environment {
        AWS_REGION = 'ap-south-1'
        ECR_REPO = 'notification-service'
        AWS_ACCOUNT_ID = '712163226335'
    }

    stages {

        stage('Get Version') {
            steps {
                script {
                    def VERSION = sh(
                        script: "mvn help:evaluate -Dexpression=project.version -q -DforceStdout | grep -v '\\[INFO\\]'",
                        returnStdout: true
                    ).trim()

                    env.VERSION = VERSION

                    echo "Project version: ${env.VERSION}"
                }
            }
        }

        stage('Build JAR') {
            steps {
                sh 'mvn -gs /var/jenkins_home/.m2/settings.xml -s /var/jenkins_home/.m2/settings.xml clean package -DskipTests'
            }
        }

        stage('Build Docker Image') {
            steps {
                sh 'docker build -t $ECR_REPO:$VERSION .'
            }
        }

        stage('Login to ECR') {
            steps {
                withCredentials([[$class: 'AmazonWebServicesCredentialsBinding', credentialsId: 'aws-creds']]) {
                    sh '''
                    aws ecr get-login-password --region $AWS_REGION | \
                    docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com
                    '''
                }
            }
        }

        stage('Tag Versioned Image') {
            steps {
                sh '''
                docker tag $ECR_REPO:$VERSION \
                $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO:$VERSION
                '''
            }
        }

        stage('Push Versioned Image') {
            steps {
                sh '''
                docker push $AWS_ACCOUNT_ID.dkr.ecr.ap-south-1.amazonaws.com/$ECR_REPO:$VERSION
                '''
            }
        }

        stage('Tag Latest') {
            steps {
                sh '''
                docker tag $ECR_REPO:$VERSION \
                $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO:latest
                '''
            }
        }

        stage('Push Latest') {
            steps {
                sh '''
                docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO:latest
                '''
            }
        }
    }
}