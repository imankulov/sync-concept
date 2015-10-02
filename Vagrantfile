# -*- mode: ruby -*-
# vi: set ft=ruby :

Vagrant.configure(2) do |config|
  config.vm.box = "ubuntu/trusty64"

  config.vm.provision "shell", inline: <<-SHELL
     sudo apt-get -qq update
     sudo apt-get install -y redis-server python-redis python-sqlalchemy python-pip python-virtualenv python-pytest ipython
  SHELL
end
