FROM gcr.io/dataflow-templates-base/python311-template-launcher-base:latest


ARG WORKDIR=/template
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR ${WORKDIR}

COPY requirements.txt .

RUN pip install --upgrade pip setuptools wheel && \
    pip install -r requirements.txt


COPY setup.py .
COPY main.py .
COPY src/ src/
COPY schemas.json .

RUN pip install -e .

ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/main.py"
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_SETUP_FILE="${WORKDIR}/setup.py"
RUN python -c "from src.parsing import ParseAndRoute, Tags; print('Imports OK')" && \
    python main.py --help > /dev/null 2>&1 || \
    (echo "ERROR: main.py --help failed" && exit 1)
