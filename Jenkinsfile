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
                sh 'git clone -b $branch http://gitlab.sq.l/connectors/jdbc-driver.git --recursive' 
                }
        }
        stage('Build'){
            steps {
                sh '''
                cd jdbc-driver
                sed -i "6s|<version>.*</version>|<version>$version_num</version>|" pom.xml
                mvn -f pom.xml package -DskipTests
                '''
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
                mv jdbc-driver/target/sqream-jdbc-$version_num-jar-with-dependencies.jar jdbc-driver/target/sqream-jdbc-$version_num.jar
                file_to_upload=$(ls -al jdbc-driver/target | grep -v dependencies | grep -i jar | awk '{ print $9 }')
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