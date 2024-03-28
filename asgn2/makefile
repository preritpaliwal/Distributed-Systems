target:
	docker build ./server -t server
	docker compose up

clean:
	docker compose down
	docker rmi load_balancer
	docker rmi server