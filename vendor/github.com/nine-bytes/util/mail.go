package util

import (
	"gopkg.in/gomail.v2"
	"html/template"
	"bytes"
)

type MailerConfig struct {
	User         string `yaml:"user"`
	Password     string `yaml:"password"`
	Server       string `yaml:"server"`
	Port         int    `yaml:"port"`
	Subject      string `yaml:"subject"`
	TemplatePath string `yaml:"template_path"`
}

type Mailer struct {
	User     string
	Password string
	Server   string
	Port     int
	Subject  string
	Template *template.Template
}

func NewMailer(config *MailerConfig) (*Mailer, error) {
	mailer := &Mailer{
		User:     config.User,
		Password: config.Password,
		Server:   config.Server,
		Port:     config.Port,
		Subject:  config.Subject,
	}

	var err error
	if mailer.Template, err = template.ParseFiles(config.TemplatePath); err != nil {
		return nil, err
	}

	return mailer, nil
}

func (mailer *Mailer) SendMail(body, to string) error {
	m := gomail.NewMessage()
	m.SetHeader("From", mailer.User)
	m.SetHeader("To", to)
	m.SetHeader("Subject", mailer.Subject)
	m.SetBody("text/html", body)
	d := gomail.NewDialer(mailer.Server, mailer.Port, mailer.User, mailer.Password)
	if err := d.DialAndSend(m); err != nil {
		return err
	}

	return nil
}

func (mailer *Mailer) Body(data interface{}) (string, error) {
	tpl := new(bytes.Buffer)
	if err := mailer.Template.Execute(tpl, data); err != nil {
		return "", err
	}

	return tpl.String(), nil
}
