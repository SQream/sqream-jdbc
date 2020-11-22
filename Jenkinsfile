if (params.version_num == "") {
    error("Please provide the version_num parameter!")
}


pipeline { 
    agent {
            label "x86_64_compilation"
            } 
    stages {
            stage("Set build number and build user"){
            steps {
                wrap([$class: 'BuildUser']){
                script {
                    currentBuild.displayName = "#${BUILD_ID}.${BUILD_USER}"
                }
                }
            }
        }
        stage('git clone jdbc') { 
            steps { 
                sh '''
                rm -rf jdbc-driver
                git clone -b $branch http://gitlab.sq.l/connectors/jdbc-driver.git --recursive
                '''
                }
        }
        stage('Build'){
            steps {
                sh """
                chmod u+x jenkins_build.sh
                ./jenkins_build.sh
                """
            }
        }
        //stage('Unit Testing'){
        //    steps {
        //        sh 'cd jdbc-driver; mvn -Dtest=JDBCTest surefire:test'               
        //    }
        //}
          stage('upload to artifactory'){
            steps {
                sh '''
                rm jdbc-driver/target/sqream-jdbc-$version_num.jar
                rm jdbc-driver/target/original-sqream-jdbc-$version_num.jar
                mv jdbc-driver/target/sqream-jdbc-$version_num-jar-with-dependencies.jar jdbc-driver/target/sqream-jdbc-$version_num.jar
        //        file_to_upload=$(ls -al jdbc-driver/target | grep -v dependencies | grep -i jar | awk '{ print $9 }')
                file_to_upload=jdbc-driver/target/sqream-jdbc-$version_num.jar
                echo $file_to_upload
                cd jdbc-driver/target/
                curl -u ${ARTIFACT_USER}:${ARTIFACT_PASSWORD} -T $file_to_upload $ARTIFACTORY_URL/connectors/jdbc/$env/
                cd ../..
                rm -rf jdbc-driver/
                '''
            }
        }
        }
}