<?php

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

namespace acdhOeaw\acdhRepoLib;

/**
 * Description of Schema
 *
 * @author zozlak
 */
class Schema {

    private $schema;

    public function __construct(object $schema) {
        $this->schema = $schema;
    }

    public function __get($name) {
        if (is_object($this->schema->$name)) {
            return json_decode(json_encode($this->schema->name));
        } else {
            return $this->schema->$name;
        }
    }

}
