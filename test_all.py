import os
from subprocess import Popen, PIPE

jdbc_repo =  '/home/sqream/jdbc-driver/'
test_src = jdbc_repo + '/src/test/java'
class_dir =  jdbc_repo + 'target/classes/com/sqream/jdbc/'
run_from  = jdbc_repo + 'target/test-classes/'

def compile_repo(repo_dir = jdbc_repo):
    ''' run maven on the repo to generate jar and class files '''

    os.chdir(repo_dir)
    compile_cmd = Popen(('mvn', 'package', '-DskipTests'), stdout = PIPE, stderr = PIPE)

    return compile_cmd.communicate()


compile_repo()   # to generate
jdbc_location = jdbc_repo + 'target/' + [jar for jar in os.listdir(jdbc_repo + 'target') if jar.endswith('dependencies.jar')][0]


def launch_sqreamd(sqream_path = '/home/sqream/master'):
    
    # Popen(('killall', '-9', 'sqreamd'))
    
    return Popen(sqream_path + '/build/sqreamd').communicate()



def run_java_test(test_name, verbose = False, jdbc_location = jdbc_location):
    ''' javac -cp .:/path/to/SQream_JDBC.jar SomeTest.java
        java -cp .:/path/to/SQream_JDBC.jar SomeTest 
    '''

    # Run a compiled test
    # Popen(('javac', '-cp' ,f'.:{jdbc_location}:{test_src}', 'com.sqream.jdbc.' + test_name + '.java'), stdout = PIPE, stderr = PIPE).communicate()
    print ('java', '-cp' ,f'.:{jdbc_location}:{run_from}', 'com.sqream.jdbc.' + test_name)
    run_test = Popen(('java', '-cp' ,f'.:{jdbc_location}:{run_from}', 'com.sqream.jdbc.' + test_name), stdout = PIPE, stderr = PIPE)
    out, err = run_test.communicate()  
    tests_ok = False

    # Check results
    failed = [res for res in out.split(b'\n') if b'Fail' in res]
    if failed or err:
        print (f"\n***\nErrors in test {test_name}:\n{failed}\n{err}")
    else:
        print (f"***\nTest: {test_name} - passed")        
        tests_ok = True
    if verbose:
        print ("Test Run details:\n",out.decode('utf8'))

    return tests_ok


if __name__ == '__main__':

    ''' Available tests: JDBC_Positive, Positive, Negative
    '''

    # launc_res, launch_err = launch_sqreamd() 
    if not all((run_java_test('JDBC_Positive'), run_java_test('Positive'), run_java_test('Negative', True))):
        raise Exception("Java tests failed")

