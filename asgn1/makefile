target:
	docker build ./server -t server
	docker compose up

clean:
	docker compose down
	sudo docker stop $(sudo docker ps -a)
	sudo docker rm $(sudo docker ps -a)
	docker rmi server
	docker rmi load_balancer