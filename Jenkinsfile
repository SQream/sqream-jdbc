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
                sh 'cd jdbc-driver/target/; curl -u ${ARTIFACT_USER}:${ARTIFACT_PASSWORD} -T sqream-jdbc-*.jar $ARTIFACTORY_URL/connectors/jdbc/release/'
                sh 'rm -rf jdbc-driver/'
            }
        }
        }
}