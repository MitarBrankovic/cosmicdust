# A container that does nothing but run loggen. The binary is built outside
# the image (static, CGO_ENABLED=0), so the image is the binary and nothing
# else: no shell, no libc, nothing to pull. experiments/lib.sh builds both.
FROM scratch
COPY loggen /loggen
ENTRYPOINT ["/loggen"]
