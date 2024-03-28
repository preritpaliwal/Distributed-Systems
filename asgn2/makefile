target:
	docker build ./server -t server
	docker compose up

clean:
	sudo docker rm -f $(sudo docker ps -qa)
	docker compose down
	docker rmi load_balancer
	docker rmi server