pipeline { 
    agent {
            label "x86_64_compilation"
            } 
    stages {
        stage('git clone jdbc') { 
            steps { 
                sh 'git clone -b $branch http://gitlab.sq.l/connectors/jdbc-driver.git --recursive' 
                }
        }
        stage('Build'){
            steps {
                sh "cd jdbc-driver; mvn -f pom.xml package -DskipTests"        
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
                file_up_upload=$(ls -al jdbc-driver/target | grep -v dependencies | grep -i jar | awk '{ print $9 }')
                echo $file_to_upload
                cd jdbc-driver/target/
                curl -u ${ARTIFACT_USER}:${ARTIFACT_PASSWORD} -T $file_to_upload $ARTIFACTORY_URL/connectors/jdbc/release/
                cd ../..
                rm -rf jdbc-driver/
                '''
            }
        }
        }
}