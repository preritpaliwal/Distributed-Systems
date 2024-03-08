target:
	docker build ./server -t server
	docker compose up

clean:
	docker compose down
	docker rmi server
	docker rmi load_balancer