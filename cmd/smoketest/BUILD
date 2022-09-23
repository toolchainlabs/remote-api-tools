go_package(name="src")

go_binary(
    name="bin",
    output_path="smoketest",
)
docker_image(
    name="smoketest",
    dependencies=[":bin"],
    instructions=[
        "FROM alpine:3.13.2",
        "COPY smoketest /usr/local/bin/",
        'ENTRYPOINT ["/usr/local/bin/smoketest"]',
    ],
)
