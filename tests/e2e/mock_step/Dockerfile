# Build context: moiraweave repo root
# so we can copy the step-sdk package from services/step-sdk/.
FROM python:3.13-slim

WORKDIR /app

# Install step-sdk from source (not published to PyPI yet)
COPY services/step-sdk/ /tmp/step-sdk/
RUN pip install --no-cache-dir /tmp/step-sdk/ uvicorn[standard]

# Copy the minimal step implementation
COPY tests/e2e/mock_step/app/ /app/

EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=3s --retries=10 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/v2/health/ready')"

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
