package main

import (
	"log"
	"os"

	"gis/polygon/services/external_controller/internal/ansible"
	"gis/polygon/services/external_controller/internal/jenkins"
	"gis/polygon/services/external_controller/internal/server"
	"gis/polygon/services/external_controller/internal/terraform"
)

func main() {
	grpcAddr := getEnv("EXTERNAL_CONTROLLER_GRPC_ADDR", ":50056")

	jenkinsURL := getEnv("JENKINS_URL", "")
	jenkinsUser := getEnv("JENKINS_USER", "")
	jenkinsToken := getEnv("JENKINS_API_TOKEN", "")

	terraformURL := getEnv("TERRAFORM_URL", "https://app.terraform.io")
	terraformToken := getEnv("TERRAFORM_TOKEN", "")
	terraformOrg := getEnv("TERRAFORM_ORGANIZATION", "")

	semaphoreURL := getEnv("SEMAPHORE_URL", "")
	semaphoreToken := getEnv("SEMAPHORE_API_TOKEN", "")

	var jenkinsClient *jenkins.Client
	if jenkinsURL != "" && jenkinsUser != "" && jenkinsToken != "" {
		jenkinsClient = jenkins.NewClient(jenkinsURL, jenkinsUser, jenkinsToken)
		log.Printf("Jenkins client configured: %s", jenkinsURL)
	}

	var terraformClient *terraform.Client
	if terraformToken != "" && terraformOrg != "" {
		terraformClient = terraform.NewClient(terraformURL, terraformToken, terraformOrg)
		log.Printf("Terraform client configured: %s (org: %s)", terraformURL, terraformOrg)
	}

	var ansibleClient *ansible.Client
	if semaphoreURL != "" && semaphoreToken != "" {
		ansibleClient = ansible.NewClient(semaphoreURL, semaphoreToken)
		log.Printf("Ansible/Semaphore client configured: %s", semaphoreURL)
	}

	srv := server.NewServer(jenkinsClient, terraformClient, ansibleClient)

	log.Printf("external_controller gRPC listening on %s", grpcAddr)
	if err := server.RunGRPC(grpcAddr, srv); err != nil {
		log.Fatalf("external_controller failed: %v", err)
	}
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
