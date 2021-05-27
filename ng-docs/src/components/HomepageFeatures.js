import React from 'react';
import clsx from 'clsx';
import styles from './HomepageFeatures.module.css';

const FeatureList = [
  {
    title: 'Choose Your Abstraction',
    Svg: require('../../static/img/undraw_docusaurus_mountain.svg').default,
    description: (
      <>
        Patio is different because it allows the developers to
        choose the level of abtraction they are comfortable with. 
      </>
    ),
  },
  {
    title: 'Database Defined Model',
    Svg: require('../../static/img/undraw_docusaurus_tree.svg').default,
    description: (
      <>
        As you add models the definition is automatically defined. 
        This is particularly useful when using a schema designed
        by another tool (i.e. ActiveRecord, Sequel, etc...)
      </>
    ),
  },
  {
    title: 'Stays Out of Your Way',
    Svg: require('../../static/img/undraw_docusaurus_react.svg').default,
    description: (
      <>
        When you define a model you have the freedom to perform any type of query
        you want.<br />
        Only want certain columns?<br />
        Want to run raw SQL?<br />
        All of that and more is available.
      </>
    ),
  },
];

function Feature({Svg, title, description}) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center">
        <Svg className={styles.featureSvg} alt={title} />
      </div>
      <div className="text--center padding-horiz--md">
        <h3>{title}</h3>
        <p>{description}</p>
      </div>
    </div>
  );
}

export default function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          {FeatureList.map((props, idx) => (
            <Feature key={idx} {...props} />
          ))}
        </div>
      </div>
    </section>
  );
}
