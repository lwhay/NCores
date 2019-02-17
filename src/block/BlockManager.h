//
// Created by Michael on 2019/2/17.
//
#pragma once

#include <iostream>
#include <fstream>
#include <boost/program_options.hpp>

using namespace std;
using namespace boost::program_options;

class BlockManager {
public:
    size_t block_size;
    size_t prefetch_size;

public:
    BlockManager() {
        ifstream conffile("../NCores.conf");
        if (conffile.fail()) {
            fstream fs;
            fs.open("../NCores.conf", ios::out);
            cout << "New conffile: " << endl;
            fs.close();
            WriteConfigurations();
        }
        ReadConfigurations();
    }

    void WriteConfigurations() {
        options_description blockconf("block");
        blockconf.add_options()("block_size", value<int>()->default_value(4096), "block_size");
        blockconf.add_options()("prefetch_size", value<int>()->default_value(1048576), "prefetch_size");
        variables_map vm;
        parsed_options po = parse_config_file<char>("../NCores.conf", blockconf, true);
        store(po, vm);
        notify(vm);
        ofstream conffile("../NCores.conf");
        for (const auto &pair : vm) {
            conffile << pair.first << "=" << vm[pair.first].as<int>() << endl;
        }
        conffile.close();
    }

    void ReadConfigurations() {
        options_description config("configurations");
        config.add_options()("block_size", value<int>());
        config.add_options()("prefetch_size", value<int>());
        variables_map vm;
        store(parse_config_file<char>("../NCores.conf", config, true), vm);
        notify(vm);
        block_size = vm["block_size"].as<int>();
        prefetch_size = vm["prefetch_size"].as<int>();
    }
};
