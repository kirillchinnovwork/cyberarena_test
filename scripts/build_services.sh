for d in ./services/*; do
	if test -f "${d}"/Dockerfile; then
	  go build -o "${d}"/bin/ "${d}"/cmd/*
	fi
done