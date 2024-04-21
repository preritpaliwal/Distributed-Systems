target:
	docker build ./server -t server
	docker build ./shard_manager -t shard_manager
	docker compose up

clean:
	docker compose down
	docker rmi load_balancer
	docker rmi server
	docker rmi shard_manager