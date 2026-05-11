package app

import _ "embed"

//go:embed seckill.lua
var embeddedSeckill string

//go:embed faq-rules.json
var embeddedFAQ []byte

func SeckillScript() string { return embeddedSeckill }

func FAQBytes() []byte { return embeddedFAQ }
