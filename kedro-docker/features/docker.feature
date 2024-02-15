@docker
Feature: Docker commands in new projects
  Background:
    Given I have prepared a config file
    And I run a non-interactive kedro new using spaceflights-pandas starter
    And I have installed the project dependencies
    And I have removed old docker image of test project

  Scenario: Execute docker init
    When I execute the kedro command "docker init"
    Then I should get a successful exit code
    And A Dockerfile file should exist
    And A .dive-ci file should exist
    And A .dockerignore file should exist

  Scenario: Execute docker build target
    When I execute the kedro command "docker build"
    Then I should get a successful exit code
    And A new docker image for test project should be created

  Scenario: Execute docker build target with custom base image
    Given I have executed kedro docker build with custom base image
    When I execute the kedro command "docker cmd python -V"
    Then I should get a successful exit code
    And A new docker image for test project should be created
    And I should get a message including "Python"

  Scenario: Execute docker run target successfully
    Given I have executed the kedro command "docker build"
    When I execute the kedro command "docker run"
    Then I should get a successful exit code
    And I should get a message including "Pipeline execution completed"

  Scenario: Execute docker run with custom base image
    When I execute kedro docker build with custom base image
    Then I should get a successful exit code
    When I execute the kedro command "docker run"
    Then I should get a successful exit code
    And I should get a message including "Pipeline execution completed"

  Scenario: Execute docker run in parallel mode
    Given I have executed the kedro command "docker build"
    When I execute the kedro command "docker run --runner=ParallelRunner"
    Then I should get a successful exit code
    And I should get a message including "Pipeline execution completed"

  Scenario: Use custom UID and GID for Docker image
    Given I have executed the kedro command "docker build --uid 10001 --gid 20002"
    When I execute the kedro command "docker cmd id"
    Then Standard output should contain a message including "uid=10001(kedro_docker) gid=20002(kedro_group) groups=20002(kedro_group)"

  Scenario: Execute docker jupyter notebook target
    Given I have executed the kedro command "docker build"
    When I execute the kedro command "docker jupyter notebook"
    Then Jupyter Server should run on port 8888

  Scenario: Execute docker jupyter notebook target on custom port
    Given I have executed the kedro command "docker build"
    When I execute the kedro command "docker jupyter notebook --port 8899"
    Then Jupyter Server should run on port 8899

  Scenario: Execute docker jupyter lab target
    Given I have executed the kedro command "docker build"
    When I execute the kedro command "docker jupyter lab"
    Then Jupyter Server should run on port 8888

  Scenario: Execute docker jupyter lab target on custom port
    Given I have executed the kedro command "docker build"
    When I execute the kedro command "docker jupyter lab --port 8899"
    Then Jupyter Server should run on port 8899

  Scenario: Jupyter port already used
    Given I have executed the kedro command "docker build"
    When I occupy port "8890"
    And I execute the kedro command "docker jupyter lab --port 8890"
    Then I should get an error exit code
    And Standard output should contain a message including "Error: Port 8890 is already in use on the host. Please specify an alternative port number."

  Scenario: Execute docker cmd without target command
    Given I have executed the kedro command "docker build"
    When I execute the kedro command "docker cmd"
    Then I should get a successful exit code
    And I should get a message including "Pipeline execution completed"

  Scenario: Execute docker cmd with non-existent target
    Given I have executed the kedro command "docker build"
    When I execute the kedro command "docker cmd kedro non-existent"
    Then Standard error should contain a message including "Error: No such command 'non-existent'"

  Scenario: Execute docker ipython target
    Given I have executed the kedro command "docker build"
    When I execute the kedro command "docker ipython"
    Then I should see messages from docker ipython startup including "An enhanced Interactive Python"
    And  I should see messages from docker ipython startup including "Kedro project project-dummy"
    And  I should see messages from docker ipython startup including "Defined global variable"
    And  I should see messages from docker ipython startup including "'context'"
    And  I should see messages from docker ipython startup including "'session'"
    And  I should see messages from docker ipython startup including "'catalog'"
    And  I should see messages from docker ipython startup including "'pipelines'"

  Scenario: Execute docker run target without building image
    When I execute the kedro command "docker run"
    Then I should get an error exit code
    And Standard output should contain a message including "Error: Unable to find image `project-dummy` locally."

  Scenario: Execute docker dive target
    Given I have executed the kedro command "docker build"
    When I execute the kedro command "docker dive"
    Then I should get a successful exit code
    And I should get a message including "Result:PASS [Total:3] [Passed:3] [Failed:0] [Warn:0] [Skipped:0]"

  Scenario: Execute docker dive with missing CI config
    Given I have executed the kedro command "docker build"
    When I execute the kedro command "docker dive -c non-existent"
    Then I should get a successful exit code
    And I should get a message including "file not found, using default CI config"
    And I should get a message including "Result:PASS [Total:3] [Passed:2] [Failed:0] [Warn:0] [Skipped:1]"

  Scenario: Execute docker dive without building image
    When I execute the kedro command "docker dive"
    Then I should get an error exit code
    And Standard output should contain a message including "Error: Unable to find image `project-dummy` locally."
