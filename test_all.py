import os
from subprocess import Popen, PIPE

# Compile repo
jdbc_repo =  '/home/eliy/jdbc-perf/'

def compile_repo(repo_dir = jdbc_repo):
    os.chdir(repo_dir)
    compile_cmd = Popen(('mvn', 'package', '-DskipTests'), stdout = PIPE, stderr = PIPE)

    return compile_cmd.communicate()


test_src = jdbc_repo + '/src/test/java'
class_dir =  jdbc_repo + 'target/classes/com/sqream/jdbc/'
tests_dir =  jdbc_repo + 'target/test-classes/com/sqream/jdbc/'
jdbc_location = jdbc_repo + 'target/sqream-jdbc-2.9.5-jar-with-dependencies.jar'

run_from = jdbc_repo + 'target/test-classes/'

def run_java_test(test_name, jdbc_location = jdbc_location):
    ''' javac -cp .:/path/to/SQream_JDBC.jar SomeTest.java
        java -cp .:/path/to/SQream_JDBC.jar SomeTest 
    '''

    # Run a compiled test
    # Popen(('javac', '-cp' ,f'.:{jdbc_location}:{test_src}', 'com.sqream.jdbc.' + test_name + '.java'), stdout = PIPE, stderr = PIPE).communicate()
    run_test = Popen(('java', '-cp' ,f'.:{jdbc_location}:{run_from}', 'com.sqream.jdbc.' + test_name), stdout = PIPE, stderr = PIPE)
    out, err = run_test.communicate()  
    
    # Check results
    failed = [res for res in out.split(b'\n') if b'Fail' in res]
    if failed or err:
        print (f"Errors in test {test_name}:\n{failed}\n{err}")
    else:
        print (f"Test {test_name} passed")        


    return out, err

if __name__ == '__main__':
    # res, err = compile_repo()
    pos_results = run_java_test('JDBC_Positive')
