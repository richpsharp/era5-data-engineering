FROM mambaorg/micromamba:1.4.2-bullseye

# Use bash so we can source and activate
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

RUN micromamba create -n hf39 -c conda-forge python=3.9 --yes && \
    micromamba clean --all --yes

# Install additional packages from conda-forge
RUN micromamba install -n hf39 -c conda-forge \
    numpy \
    pandas \
    \--yes

RUN micromamba install -n hf39 -c conda-forge xarray netcdf4
RUN micromamba clean --all --yes


# Put the environment on PATH
ENV PATH=/opt/conda/envs/hf39/bin:$PATH

# Optional: have it auto-activate when you enter a shell
RUN echo 'micromamba activate hf39' >> /home/mambauser/.bashrc

WORKDIR /home/mambauser/data
USER mambauser
CMD ["/bin/bash"]
